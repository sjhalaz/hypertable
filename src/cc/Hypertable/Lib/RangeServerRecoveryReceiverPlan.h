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

#ifndef HYPERTABLE_RANGESERVERRECOVERYRECEIVERPLAN_H
#define HYPERTABLE_RANGESERVERRECOVERYRECEIVERPLAN_H

#include "Common/Compat.h"
#include <vector>
#include <iostream>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/mem_fun.hpp>

#include "Common/StringExt.h"
#include "Common/PageArenaAllocator.h"

#include "Types.h"

namespace Hypertable {
  using namespace boost::multi_index;
  using namespace std;

  class RangeServerRecoveryReceiverPlan {
  public:
    RangeServerRecoveryReceiverPlan() { }
    void insert(const char *location, const TableIdentifier &table, const RangeSpec &range,
        const RangeState &state);
    void get_locations(StringSet &locations) const;
    bool get_location(const TableIdentifier &table, const char *row, String &location) const;
    void get_ranges(vector<QualifiedRangeStateSpecManaged> &ranges) const;
    void get_ranges(const char *location, vector<QualifiedRangeStateSpec> &ranges);
    void get_ranges(const char *location, vector<QualifiedRangeSpec> &ranges);
    void get_ranges(const char *location, vector<QualifiedRangeStateSpecManaged> &ranges) const;
    bool get_range(const TableIdentifier &table, const char *row,
        QualifiedRangeStateSpec &range);
    bool get_range(const TableIdentifier &table, const char *row,
        QualifiedRangeSpec &range);
    void erase_range(const QualifiedRangeSpec &range);

    size_t encoded_length() const;
    void encode(uint8_t **bufp) const;
    void decode(const uint8_t **bufp, size_t *remainp);

  private:
    class ReceiverEntry {
    public:
      ReceiverEntry(CharArena &arena, const String &location_,
          const QualifiedRangeStateSpecManaged &state_spec_) {
        location = arena.dup(location_.c_str());
        state_spec.qualified_range.table.id = arena.dup(state_spec_.qualified_range.table.id);
        state_spec.qualified_range.table.generation =
            state_spec_.qualified_range.table.generation;
        state_spec.qualified_range.range.start_row =
            arena.dup(state_spec_.qualified_range.range.start_row);
        state_spec.qualified_range.range.end_row =
            arena.dup(state_spec_.qualified_range.range.end_row);
        state_spec.state.state = state_spec_.state.state;
        state_spec.state.timestamp = state_spec_.state.timestamp;
        state_spec.state.soft_limit = state_spec_.state.soft_limit;
        state_spec.state.transfer_log = arena.dup(state_spec_.state.transfer_log);
        state_spec.state.split_point = arena.dup(state_spec_.state.split_point);
        state_spec.state.old_boundary_row = arena.dup(state_spec_.state.old_boundary_row);
      }

      ReceiverEntry(CharArena &arena, const char *location_,
          const TableIdentifier &table_, const RangeSpec &range_, const RangeState &state_) {
        location = arena.dup(location_);
        state_spec.qualified_range.table.id = arena.dup(table_.id);
        state_spec.qualified_range.table.generation = table_.generation;
        state_spec.qualified_range.range.start_row = arena.dup(range_.start_row);
        state_spec.qualified_range.range.end_row = arena.dup(range_.end_row);
        state_spec.state.state = state_.state;
        state_spec.state.timestamp = state_.timestamp;
        state_spec.state.soft_limit = state_.soft_limit;
        state_spec.state.transfer_log = arena.dup(state_.transfer_log);
        state_spec.state.split_point = arena.dup(state_.split_point);
        state_spec.state.old_boundary_row = arena.dup(state_.old_boundary_row);
      }

      ReceiverEntry() : location(0) { }

      size_t encoded_length() const;
      void encode(uint8_t **bufp) const;
      void decode(const uint8_t **bufp, size_t *remainp);

      const char *location;
      QualifiedRangeStateSpec state_spec;
      friend ostream &operator<< (ostream &os, const ReceiverEntry &entry) {
        os << "{ReceiverEntry:" << entry.state_spec << ", location=" << entry.location << "}";
        return os;
      }
    };


    struct ByRange {};
    struct ByLocation {};
    typedef multi_index_container<
      ReceiverEntry,
      indexed_by<
        ordered_unique<tag<ByRange>, member<ReceiverEntry, QualifiedRangeStateSpec,
                       &ReceiverEntry::state_spec> >,
        ordered_non_unique<tag<ByLocation>,
                           member<ReceiverEntry, const char*, &ReceiverEntry::location>,
                           LtCstr>
      >
    > ReceiverPlan;
    typedef ReceiverPlan::index<ByRange>::type RangeIndex;
    typedef ReceiverPlan::index<ByLocation>::type LocationIndex;

    ReceiverPlan m_plan;
    CharArena m_arena;

  public:
    friend ostream &operator<<(ostream &os, const RangeServerRecoveryReceiverPlan &plan) {
      RangeServerRecoveryReceiverPlan::LocationIndex &location_index =
          (const_cast<RangeServerRecoveryReceiverPlan&>(plan)).m_plan.get<1>();
      RangeServerRecoveryReceiverPlan::LocationIndex::const_iterator location_it =
        location_index.begin();
      os << "{RangeServerRecoveryReceiverPlan: num_entries=" << location_index.size();
      while(location_it != location_index.end()) {
        os << *location_it;
        ++location_it;
      }
      os << "}";
      return os;
    }
  };


} // namespace Hypertable


#endif // HYPERTABLE_RANGESERVERRECOVERYRECEIVERPLAN_H
