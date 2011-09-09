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

#ifndef HYPERTABLE_REPLAYBUFFER_H
#define HYPERTABLE_REPLAYBUFFER_H

#include <map>
#include "Common/Properties.h"
#include "Common/ReferenceCount.h"
#include "AsyncComm/Comm.h"

#include "Hypertable/Lib/Types.h"
#include "Hypertable/Lib/RangeServerRecoveryLoadPlan.h"
#include "RangeReplayBuffer.h"

namespace Hypertable {
  using namespace std;
  /**
   *
   */
  class ReplayBuffer : public ReferenceCount {
  public:
    ReplayBuffer(PropertiesPtr &props, Comm *comm, RangeServerRecoveryLoadPlan &plan);

    void add(const TableIdentifier &table, SerializedKey &key, ByteString &value);
    size_t memory_used() const { return m_memory_used; }
    size_t num_entries() const { return m_num_entries; }
    void finish_fragment();
    void set_current_fragment(uint32_t fragment_id) {m_fragment = fragment_id;}

  private:
    void flush();
    Comm *m_comm;
    RangeServerRecoveryLoadPlan &m_load_plan;
    typedef map<QualifiedRangeSpec, RangeReplayBufferPtr> ReplayBufferMap;
    set<QualifiedRangeSpec> m_completed_ranges;
    ReplayBufferMap m_buffer_map;
    String m_location;
    size_t m_memory_used;
    size_t m_num_entries;
    size_t m_flush_limit_aggregate;
    size_t m_flush_limit_per_range;
    int32_t m_timeout_ms;
    uint32_t m_fragment;
  };

  typedef intrusive_ptr<ReplayBuffer> ReplayBufferPtr;

} // namespace Hypertable

#endif // HYPERTABLE_REPLAYBUFFER_H
