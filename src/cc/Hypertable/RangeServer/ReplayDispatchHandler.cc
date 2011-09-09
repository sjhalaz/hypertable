/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
#include "ReplayDispatchHandler.h"

using namespace std;
using namespace Hypertable;

void ReplayDispatchHandler::handle(Hypertable::EventPtr &event_ptr) {
  int32_t error;
  bool range_error=false;
  bool location_error=false;
  QualifiedRangeSpec range;
  String error_msg;

  if (event_ptr->type == Event::MESSAGE) {
    error = Protocol::response_code(event_ptr);
    const uint8_t *decode_ptr = event_ptr->payload + 4;
    size_t decode_remain = event_ptr->payload_len - 4;

    if (error != Error::OK) {
      error_msg = Serialization::decode_str16(&decode_ptr, &decode_remain);
      range.decode(&decode_ptr, &decode_remain);
      if (error == Error::RANGESERVER_FRAGMENT_COMPLETELY_RECEIVED) {
        HT_WARN_OUT << error_msg << HT_END;
        m_completed_ranges.insert(range);
      }
      else {
        range_error = true;
        HT_ERROR_OUT << "replay failure for range " << range << " at " << event_ptr->proxy
                     << "-" << error_msg << HT_END;
      }
    }
  }
  else {
    location_error = true;
    HT_ERROR_OUT << "replay failure for at " << event_ptr->proxy << ", error="
        << event_ptr->error << " - " << event_ptr->to_str() << HT_END;
  }

  {
    ScopedLock lock(m_mutex);
    HT_ASSERT(m_outstanding>0);

    if (location_error)
      m_location_errors[event_ptr->proxy] = event_ptr->error;
    else if (range_error)
      m_range_errors[range] = error;
    m_outstanding--;
    if (m_outstanding == 0)
      m_cond.notify_all();
  }
}

void ReplayDispatchHandler::add(const CommAddress &addr,
    const QualifiedRangeSpec &range, uint32_t fragment, bool more, StaticBuffer &buffer) {
  try {
    ScopedLock lock(m_mutex);
    m_outstanding++;
    m_rsclient.phantom_update(addr, m_recover_location, range, fragment, more, buffer, this);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << "Error sending phantom updates for range " << range << " to "
        << addr.to_str() << "-" << e << HT_END;
  }
}

void ReplayDispatchHandler::get_error_ranges(vector<QualifiedRangeSpec> &ranges ) {
  HT_ASSERT(m_outstanding==0);
  map<QualifiedRangeSpecManaged, int32_t>::iterator it = m_range_errors.begin();
  for(; it!= m_range_errors.end(); ++it) {
    ranges.push_back(it->first);
  }
}

void ReplayDispatchHandler::get_completed_ranges(set<QualifiedRangeSpec> &ranges) {
  HT_ASSERT(m_outstanding==0);
  set<QualifiedRangeSpecManaged>::iterator it = m_completed_ranges.begin();
  while(it != m_completed_ranges.end()) {
    ranges.insert(*it);
    ++it;
  }
}

void ReplayDispatchHandler::get_error_locations(vector<String> &locations) {
  HT_ASSERT(m_outstanding==0);
  map<String, int32_t>::iterator it = m_location_errors.begin();
  for(; it!= m_location_errors.end(); ++it) {
    locations.push_back(it->first);
  }
}

bool ReplayDispatchHandler::wait_for_completion() {
  ScopedLock lock(m_mutex);
  while(m_outstanding) {
    m_cond.wait(lock);
  }
  return (m_range_errors.size()>0 || m_location_errors.size()>0);
}

