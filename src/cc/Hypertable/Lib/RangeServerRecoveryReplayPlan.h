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

#ifndef HYPERTABLE_RANGESERVERRECOVERYREPLAYPLAN_H
#define HYPERTABLE_RANGESERVERRECOVERYREPLAYPLAN_H

#include "Common/Compat.h"
#include <vector>
#include <iostream>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/member.hpp>

#include "Common/StringExt.h"

namespace Hypertable {
  using namespace boost::multi_index;
  using namespace std;

  class RangeServerRecoveryReplayPlan {
  public:
    RangeServerRecoveryReplayPlan() { }

    void insert(const char *location, uint32_t fragment);
    void get_fragments(vector<uint32_t> &fragments) const;
    void get_fragments(const char *location, vector<uint32_t> &fragments) const;
    void get_locations(StringSet &locations) const;
    bool get_location(uint32_t fragment, String &location) const;

    size_t encoded_length() const;
    void encode(uint8_t **bufp) const;
    void decode(const uint8_t **bufp, size_t *remainp);

  private:
    class ReplayEntry {
      public:
        ReplayEntry(const String &location_, uint32_t fragment_) : location(location_),
        fragment(fragment_) { }
        ReplayEntry(const String &location_) : location(location_), fragment(0) { }
        ReplayEntry(const char *location_) : location(location_), fragment(0) { }
        ReplayEntry(uint32_t fragment_) : fragment(fragment_) { }
        ReplayEntry() : fragment(0) { }

        size_t encoded_length() const;
        void encode(uint8_t **bufp) const;
        void decode(const uint8_t **bufp, size_t *remainp);

        String location;
        uint32_t fragment;
        friend ostream &operator<<(ostream &os, const ReplayEntry &entry) {
          os << "{ReplayEntry:location=" << entry.location<< ", fragment="
             << entry.fragment<< "}";
          return os;
        }
    };

    struct ByFragment {};
    struct ByLocation {};
    typedef boost::multi_index_container<
      ReplayEntry,
      indexed_by<
        ordered_unique<tag<ByFragment>,
                       member<ReplayEntry, uint32_t, &ReplayEntry::fragment> >,
        ordered_non_unique<tag<ByLocation>,
                           member<ReplayEntry, String, &ReplayEntry::location> >
      >
    > ReplayPlan;
    typedef ReplayPlan::index<ByFragment>::type FragmentIndex;
    typedef ReplayPlan::index<ByLocation>::type LocationIndex;

    ReplayPlan m_plan;

  public:
    friend ostream &operator<<(ostream &os, const RangeServerRecoveryReplayPlan &plan) {
      RangeServerRecoveryReplayPlan::LocationIndex &index =
          (const_cast<RangeServerRecoveryReplayPlan&>(plan)).m_plan.get<1>();
      RangeServerRecoveryReplayPlan::LocationIndex::const_iterator iter = index.begin();

      os << "{RangeServerRecoveryReplayPlan: num_entries=" << index.size();
      while(iter!=index.end()) {
        os << *iter;
        ++iter;
      }
      os << "}";
      return os;
    }
  };


} // namespace Hypertable


#endif // HYPERTABLE_RANGESERVERRECOVERYREPLAYPLAN_H
