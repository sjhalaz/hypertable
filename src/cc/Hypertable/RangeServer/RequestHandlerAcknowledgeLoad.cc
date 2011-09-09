/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
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

#include "ResponseCallbackAcknowledgeLoad.h"
#include "Common/Serialization.h"

#include "Hypertable/Lib/Types.h"

#include "RangeServer.h"
#include "RequestHandlerAcknowledgeLoad.h"
#include "ResponseCallbackAcknowledgeLoad.h"

using namespace Hypertable;
using namespace Serialization;

/**
 *
 */
void RequestHandlerAcknowledgeLoad::run() {
  ResponseCallbackAcknowledgeLoad cb(m_comm, m_event_ptr);
  vector<QualifiedRangeSpec> ranges;
  QualifiedRangeSpec range;
  const uint8_t *decode_ptr = m_event_ptr->payload;
  size_t decode_remain = m_event_ptr->payload_len;
  int nn;

  try {
    nn = Serialization::decode_i32(&decode_ptr, &decode_remain);
    for (int ii=0; ii<nn; ++ii) {
      range.decode(&decode_ptr, &decode_remain);
      ranges.push_back(range);
    }
    m_range_server->acknowledge_load(&cb, ranges);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << "AcknowledgeLoad " << e << HT_END;
    cb.error(e.code(), "Error handling AcknowledgeLoad message");
  }
}
