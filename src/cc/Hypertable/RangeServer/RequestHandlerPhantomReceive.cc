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
#include "Common/Error.h"
#include "Common/Logger.h"

#include "AsyncComm/ResponseCallback.h"
#include "Common/Serialization.h"

#include "Hypertable/Lib/Types.h"

#include "RangeServer.h"
#include "RequestHandlerPhantomReceive.h"

using namespace Hypertable;
using namespace Serialization;

/**
 *
 */
void RequestHandlerPhantomReceive::run() {
  ResponseCallback cb(m_comm, m_event_ptr);
  String location;
  vector<uint32_t> fragments;
  vector<QualifiedRangeStateSpec> ranges;
  uint32_t nn;

  const uint8_t *decode_ptr = m_event_ptr->payload;
  size_t decode_remain = m_event_ptr->payload_len;
  QualifiedRangeStateSpec range;

  try {
    location = Serialization::decode_vstr(&decode_ptr, &decode_remain);
    nn = Serialization::decode_i32(&decode_ptr, &decode_remain);
    for(uint32_t ii=0; ii<nn; ++ii)
      fragments.push_back(Serialization::decode_vi32(&decode_ptr, &decode_remain));
    nn = Serialization::decode_i32(&decode_ptr, &decode_remain);
    for(uint32_t ii=0; ii<nn; ++ii) {
      range.decode(&decode_ptr, &decode_remain);
      ranges.push_back(range);
    }
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb.error(e.code(), e.what());
    return;
  }
  try {
    m_range_server->phantom_receive(&cb, location, fragments, ranges);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }

}
