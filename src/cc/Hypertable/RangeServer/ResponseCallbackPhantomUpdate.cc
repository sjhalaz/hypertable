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
#include "ResponseCallbackPhantomUpdate.h"

using namespace Hypertable;

int ResponseCallbackPhantomUpdate::response() {
  CommHeader header;
  header.initialize_from_request_header(m_event_ptr->header);
  size_t len = m_range.encoded_length() + Serialization::encoded_length_vi32(m_fragment) + 4;
  CommBufPtr cbp(new CommBuf(header, len));
  cbp->append_i32(Error::OK);
  m_range.encode(cbp->get_data_ptr_address());
  Serialization::encode_vi32(cbp->get_data_ptr_address());
  return m_comm->send_response(m_event_ptr->addr, cbp);
}

int ResponseCallbackPhantomUpdate::response_ok() {
  return response();
}

int ResponseCallbackPhantomUpdate::error(int error, const String &msg) {
  CommHeader header;
  header.initialize_from_request_header(m_event_ptr->header);
  CommBufPtr cbp;
  String message;
  size_t max_msg_size = std::numeric_limits<int16_t>::max();
  size_t len = m_range.encoded_length() + Serialization::encoded_length_vi32(m_fragment);

  if (msg.length() < max_msg_size) {
    len += msg.length();
    cbp = new CommBuf(header, len);
    cbp->append_i32(error);
    cbp->append_str16(msg);
  }
  else {
    len += max_msg_size;
    cbp = new CommBuf(header, len);
    cbp->append_i32(error);
    cbp->append_str16(msg.substr(0, max_msg_size));
  }
  m_range.encode(cbp->get_data_ptr_address());
  Serialization::encode_vi32(cbp->get_data_ptr_address());

  return m_comm->send_response(m_event_ptr->addr, cbp);
}

