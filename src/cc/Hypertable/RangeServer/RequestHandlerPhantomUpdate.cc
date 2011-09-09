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
#include "RequestHandlerPhantomUpdate.h"
#include "ResponseCallbackPhantomUpdate.h"

using namespace Hypertable;
using namespace Serialization;

/**
 *
 */
void RequestHandlerPhantomUpdate::run() {
  String location;
  vector<uint32_t> fragments;
  vector<QualifiedRangeSpecManaged> ranges;
  uint32_t nn;
  ResponseCallback *cb;

  const uint8_t *decode_ptr = m_event_ptr->payload;
  size_t decode_remain = m_event_ptr->payload_len;
  String location;
  QualifiedRangeSpec range;
  uint32_t fragment;
  bool more;

  try {
    location = Serialization::decode_vstr(&decode_ptr, &decode_remain);
    range.decode(&decode_ptr, &decode_remain);
    fragment = Serialization::decode_vi32(&decode_ptr, &decode_remain);
    more = Serialization::decode_bool(&decode_ptr, &decode_remain);
    cb = new ResponseCallbackPhantomUpdate(m_comm, m_event_ptr, range, fragment);
    m_range_server->phantom_update(cb, location, range, fragment, m_event_ptr);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb->error(e.code(), e.what());
    return;
  }
}
