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

#include "FragmentData.h"

using namespace std;
using namespace Hypertable;

void FragmentData::add(bool more, EventPtr &event) {
  m_data.push_back(event);
  m_done = !more;
  return;
}

void FragmentData::merge(RangePtr &range, DynamicBuffer &dbuf, int *latest_revision) {
  QualifiedRangeSpec range;
  uint32_t fragment;
  bool more;
  StaticBuffer buffer;
  Key key;
  SerializedKey serkey;
  ByteString value;

  *latest_revision = TIMESTAMP_MIN;
  foreach(EventPtr &event, m_data) {
    const uint8_t *decode_ptr = m_event_ptr->payload;
    size_t decode_remain = m_event_ptr->payload_len;
    range.decode(&decode_ptr, &decode_remain);
    fragment = Serialization::decode_vi32(&decode_ptr, &decode_remain);
    more = Serialization::decode_bool(&decode_ptr, &decode_remain);

    buffer.base = (uint8_t *)decode_ptr;
    buffer.size = decode_remain;
    buffer.own = false;
    const uint8_t *mod, *mod_end;
    mod_end = buffer.base + buffer.size;
    mod = buffer.base;

    while (mod<end) {
      serkey.ptr = mod;
      value.ptr = mod + serkey.length();
      HT_ASSERT(serkey.ptr <= mod_end && value.ptr <= mod_end);
      HT_ASSERT(key.load(serkey));
      if (key.revision > *latest_revision)
        *latest_revision = revision;
      range->add(key, value);
      // skip to next kv pair
      value.next();
      mod = value.ptr;
    }
    dbuf.ensure(buffer.size);
    dbuf.add_unchecked((const void *)buffer.base, buffer.size);
  }
}
