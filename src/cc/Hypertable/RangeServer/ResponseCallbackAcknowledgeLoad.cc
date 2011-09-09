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
#include "ResponseCallbackAcknowledgeLoad.h"

using namespace std;
using namespace Hypertable;

int ResponseCallbackAcknowledgeLoad::response(const map<QualifiedRangeSpec, int> &error_map) {
  CommHeader header;
  header.initialize_from_request_header(m_event_ptr->header);
  size_t len = 8;
  map<QualifiedRangeSpec, int>::const_iterator map_it = error_map.begin();
  while(map_it != error_map.end()) {
    len += map_it->first.encoded_length();
    len += Serialization::encoded_length_vi32(map_it->second);
    ++map_it;
  }
  CommBufPtr cbp(new CommBuf( header, len));
  cbp->append_i32(Error::OK);
  cbp->append_i32(error_map.size());
  map_it = error_map.begin();
  while(map_it != error_map.end()) {
    map_it->first.encode(cbp->get_data_ptr_address());
    Serialization::encode_vi32(cbp->get_data_ptr_address(), map_it->second);
    ++map_it;
  }
  return m_comm->send_response(m_event_ptr->addr, cbp);
}
