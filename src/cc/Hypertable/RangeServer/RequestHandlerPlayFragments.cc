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
#include "Hypertable/Lib/RangeServerRecoveryLoadPlan.h"

#include "RangeServer.h"
#include "RequestHandlerPlayFragments.h"

using namespace Hypertable;
using namespace Serialization;

/**
 *
 */
void RequestHandlerPlayFragments::run() {
  ResponseCallback cb(m_comm, m_event_ptr);
  int64_t op_id;
  int type;
  uint32_t attempt, replay_timeout;
  String recover_location;
  vector<uint32_t> fragments;
  RangeServerRecoveryLoadPlan load_plan;
  uint32_t nn;

  const uint8_t *decode_ptr = m_event_ptr->payload;
  size_t decode_remain = m_event_ptr->payload_len;

  try {
    op_id = Serialization::decode_vi64(&decode_ptr, &decode_remain);
    attempt = Serialization::decode_vi32(&decode_ptr, &decode_remain);
    recover_location = Serialization::decode_vstr(&decode_ptr, &decode_remain);
    type = Serialization::decode_vi32(&decode_ptr, &decode_remain);
    nn = Serialization::decode_i32(&decode_ptr, &decode_remain);
    for(uint32_t ii=0; ii<nn; ++ii)
      fragments.push_back(Serialization::decode_vi32(&decode_ptr, &decode_remain));
    load_plan.decode(&decode_ptr, &decode_remain);
    replay_timeout = Serialization::decode_i32(&decode_ptr, &decode_remain);

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb.error(e.code(), e.what());
    return;
  }

  try {
    m_range_server->play_fragments(&cb, op_id, attempt, recover_location, type,
        fragments, load_plan, replay_timeout);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }
}
