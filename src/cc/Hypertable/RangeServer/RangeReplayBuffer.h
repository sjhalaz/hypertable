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

#ifndef HYPERTABLE_RANGEREPLAYBUFFER_H
#define HYPERTABLE_RANGEREPLAYBUFFER_H

#include "Common/ReferenceCount.h"
#include "Common/StringExt.h"
#include "Common/DynamicBuffer.h"
#include "Common/StaticBuffer.h"
#include "Common/Properties.h"

#include "AsyncComm/CommAddress.h"

#include "Hypertable/Lib/Types.h"
#include "Hypertable/Lib/SerializedKey.h"

namespace Hypertable {

  /**
   *
   */
  class RangeReplayBuffer : public ReferenceCount {
  public:
    RangeReplayBuffer(const String &location, const QualifiedRangeSpec &range) :
        m_location(location), m_range(range), m_memory_used(0), m_num_entries(0) {
      m_addr.set_proxy(location);
    }
    String get_location() const {return m_location; }
    size_t memory_used() const { return m_accum.fill(); }
    size_t num_entries() const { return m_num_entries; }
    size_t add(SerializedKey &key, ByteString &value);
    void get_updates(StaticBuffer &updates) { updates = m_accum; }
    void clear();
    CommAddress& get_comm_address() { return m_addr; }
    QualifiedRangeSpec& get_range() { return m_range; }

  private:
    String m_location;
    CommAddress m_addr;
    QualifiedRangeSpec m_range;
    size_t m_memory_used;
    size_t m_num_entries;
    DynamicBuffer m_accum;
  };

  typedef intrusive_ptr<RangeReplayBuffer> RangeReplayBufferPtr;

} // namespace Hypertable

#endif // HYPERTABLE_RANGEREPLAYBUFFER_H
