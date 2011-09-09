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

#ifndef HYPERTABLE_RANGESERVERRECOVERYLOADPLAN_H
#define HYPERTABLE_RANGESERVERRECOVERYLOADPLAN_H

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
  //used by master to store which servers are loading what range
  //used by replay locations to direct an update to its correct location
  //must support
  //1.given a location, what ranges was it loading
  //2.given a table + key, what location is it on
  //3.modify location
  //4.encode, decode

  class RangeServerRecoveryLoadPlan {
  public:
    RangeServerRecoveryLoadPlan() { }
    void insert(const char *location, const TableIdentifier &table, const RangeSpec &range);
    void get_locations(StringSet &locations) const;
    bool get_location(const TableIdentifier &table, const char *row, String &location) const;
    void get_ranges(vector<QualifiedRangeSpecManaged> &ranges) const;
    void get_ranges(const char *location, vector<QualifiedRangeSpec*> &ranges);
    void get_ranges(const char *location, vector<QualifiedRangeSpec> &ranges);
    void get_ranges(const char *location, vector<QualifiedRangeSpecManaged> &ranges) const;
    bool get_range(const TableIdentifier &table, const char *row,
        QualifiedRangeSpec &range);
    void erase_range(const QualifiedRangeSpec &range);

    size_t encoded_length() const;
    void encode(uint8_t **bufp) const;
    void decode(const uint8_t **bufp, size_t *remainp);

  private:
    class LoadEntry {
    public:
      LoadEntry(CharArena &arena, const String &location_,
          const QualifiedRangeSpecManaged &range_) {
        location = arena.dup(location_.c_str());
        range.table.id = arena.dup(range_.table.id);
        range.table.generation = range_.table.generation;
        range.range.start_row = arena.dup(range_.range.start_row);
        range.range.end_row = arena.dup(range_.range.end_row);
      }

      LoadEntry(CharArena &arena, const char *location_,
          const TableIdentifier &table_, const RangeSpec &range_) {
        location = arena.dup(location_);
        range.table.id = arena.dup(table_.id);
        range.table.generation = table_.generation;
        range.range.start_row = arena.dup(range_.start_row);
        range.range.end_row = arena.dup(range_.end_row);
      }

      LoadEntry() : location(0) { }

      size_t encoded_length() const;
      void encode(uint8_t **bufp) const;
      void decode(const uint8_t **bufp, size_t *remainp);

      const char *location;
      QualifiedRangeSpec range;
      friend ostream &operator<< (ostream &os, const LoadEntry &entry) {
        os << "{LoadEntry:" << entry.range << ", location=" << entry.location << "}";
        return os;
      }
    };


    struct ByRange {};
    struct ByLocation {};
    typedef multi_index_container<
      LoadEntry,
      indexed_by<
        ordered_unique<tag<ByRange>, member<LoadEntry, QualifiedRangeSpec, &LoadEntry::range> >,
        ordered_non_unique<tag<ByLocation>,
                           member<LoadEntry, const char*, &LoadEntry::location>,
                           LtCstr>
      >
    > LoadPlan;
    typedef LoadPlan::index<ByRange>::type RangeIndex;
    typedef LoadPlan::index<ByLocation>::type LocationIndex;

    LoadPlan m_plan;
    CharArena m_arena;
  public:
    friend ostream &operator<<(ostream &os, const RangeServerRecoveryLoadPlan &plan) {
      RangeServerRecoveryLoadPlan::LocationIndex &location_index =
          (const_cast<RangeServerRecoveryLoadPlan&>(plan)).m_plan.get<1>();
      RangeServerRecoveryLoadPlan::LocationIndex::const_iterator location_it =
        location_index.begin();
      os << "{RangeServerRecoveryLoadPlan: num_entries=" << location_index.size();
      while(location_it != location_index.end()) {
        os << *location_it;
        ++location_it;
      }
      os << "}";
      return os;
    }
  };


} // namespace Hypertable


#endif // HYPERTABLE_RANGESERVERRECOVERYLOADPLAN_H
