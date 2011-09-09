/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc
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
#include "ReplayBuffer.h"
#include "ReplayDispatchHandler.h"

using namespace std;
using namespace Hypertable;
using namespace Hypertable::Property;

ReplayBuffer::ReplayBuffer(PropertiesPtr &props, Comm *comm,
    RangeServerRecoveryReceiverPlan &plan, const String &location) : m_comm(comm), m_plan(plan),
    m_location(location), m_memory_used(0), m_num_entries(0) {
  m_flush_limit_aggregate =
      (size_t)props->get_i64("Hypertable.RangeServer.Failover.FlushLimit.Aggregate");
  m_flush_limit_per_range =
      (size_t)props->get_i32("Hypertable.RangeServer.Failover.FlushLimit.PerRange");
  m_timeout_ms = props->get_i32("Hypertable.Failover.Timeout");
  StringSet locations;
  m_plan.get_locations(locations);
  foreach(const String &location, locations) {
    vector<QualifiedRangeStateSpec> ranges;
    m_plan.get_ranges(location.c_str(), ranges);
    foreach(QualifiedRangeStateSpec &range, ranges) {
      RangeReplayBufferPtr replay_buffer
          = new RangeReplayBuffer(location, range.qualified_range);
      m_buffer_map[range.qualified_range] = replay_buffer;
    }
  }
}

void ReplayBuffer::add(const TableIdentifier &table, SerializedKey &key, ByteString &value) {
  const char *row = key.row();
  QualifiedRangeSpec range;
  // skip over any cells that are not in the recovery plan
  if (m_plan.get_range(table, row, range)) {
    // skip over any ranges that have completely received data for this fragment
    if (m_completed_ranges.find(range) != m_completed_ranges.end()) {
      HT_DEBUG_OUT << "Skipping key " << row << " which is in completed range"
          << range << HT_END;
      return;
    }
    ReplayBufferMap::iterator it = m_buffer_map.find(range);
    HT_ASSERT(it != m_buffer_map.end());
    m_memory_used += it->second->add(key, value);
    m_num_entries++;
    if (m_memory_used > m_flush_limit_aggregate ||
       it->second->memory_used() > m_flush_limit_per_range) {
       //HT_DEBUG_OUT << "flushing replay buffer for fragment " << m_fragment
       //    << ", total mem=" << m_memory_used << " range mem used=" << it->second->memory_used()
       //    << ", total limit=" << m_flush_limit_aggregate << ", per range limit="
       //    << m_flush_limit_per_range << " key=" << row << HT_END;
       flush();
    }
  }
  else {
    HT_DEBUG_OUT << "Skipping key " << row << " for table " << table.id
        << " because it is not in recovery plan" << HT_END;
  }
}

void ReplayBuffer::flush() {
  ReplayDispatchHandler handler(m_comm, m_location, m_timeout_ms);

  foreach(ReplayBufferMap::value_type &vv, m_buffer_map) {
    // skip over any ranges that have completely received data for this fragment
    if (m_completed_ranges.find(vv.first) != m_completed_ranges.end())
      continue;

    if (vv.second->memory_used() > 0) {
      RangeReplayBuffer &buffer = *(vv.second.get());
      CommAddress &addr         = buffer.get_comm_address();
      QualifiedRangeSpec &range = buffer.get_range();
      StaticBuffer updates;
      buffer.get_updates(updates);
      handler.add(addr, range, m_fragment, true, updates);
      buffer.clear();
    }
  }
  if (!handler.wait_for_completion()) {
    vector<String> locations;
    vector<QualifiedRangeSpec> ranges;
    handler.get_error_ranges(ranges);
    handler.get_error_locations(locations);
    foreach(const String &location, locations)
      m_plan.get_ranges(location.c_str(), ranges);
    foreach(QualifiedRangeSpec range, ranges) {
      m_buffer_map.erase(range);
      m_plan.erase_range(range);
    }
  }
  // update set of ranges that have already finished receiving data for this fragment
  set<QualifiedRangeSpec> completed_ranges;
  set<QualifiedRangeSpec>::iterator set_it;
  handler.get_completed_ranges(completed_ranges);
  set_it = completed_ranges.begin();
  while (set_it != completed_ranges.end()) {
    ReplayBufferMap::iterator it = m_buffer_map.find(*set_it);
    if (it != m_buffer_map.end()) {
      m_completed_ranges.insert(it->first);
      it->second->clear();
    }
    ++set_it;
  }
  m_memory_used=0;
}

void ReplayBuffer::finish_fragment() {
  ReplayDispatchHandler handler(m_comm, m_location, m_timeout_ms);
  foreach(ReplayBufferMap::value_type &vv, m_buffer_map) {
    // skip over any ranges that have completely received data for this fragment
    if (m_completed_ranges.find(vv.first) != m_completed_ranges.end())
      continue;

    RangeReplayBuffer &buffer=*(vv.second.get());
    CommAddress &addr=buffer.get_comm_address();
    QualifiedRangeSpec &range = buffer.get_range();
    StaticBuffer updates;
    buffer.get_updates(updates);
    handler.add(addr, range, m_fragment, false, updates);
    HT_DEBUG_OUT << "Finished replaying fragment " << m_fragment << " to range "
        << range << " to location " << addr.to_str() << HT_END;
    buffer.clear();
  }
  if (!handler.wait_for_completion()) {
    vector<String> locations;
    vector<QualifiedRangeSpec> ranges;
    handler.get_error_ranges(ranges);
    handler.get_error_locations(locations);
    foreach(const String &location, locations)
      m_plan.get_ranges(location.c_str(), ranges);
    foreach(QualifiedRangeSpec &range, ranges) {
      m_buffer_map.erase(range);
      m_plan.erase_range(range);
    }
  }
  HT_DEBUG_OUT << "Read " << m_num_entries << " k/v pairs from fragment " << m_fragment
      << HT_END;
  m_completed_ranges.clear();
  m_memory_used=0;
  m_num_entries=0;
}
