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
#include "RangeServerRecoveryReceiverPlan.h"

using namespace Hypertable;
using namespace boost::multi_index;
using namespace std;

void RangeServerRecoveryReceiverPlan::insert(const char *location,
    const TableIdentifier &table, const RangeSpec &range, const RangeState &state) {
  ReceiverEntry entry(m_arena, location, table, range, state);
  RangeIndex &range_index = m_plan.get<ByRange>();
  RangeIndex::iterator range_it = range_index.find(entry.state_spec);
  if (range_it != range_index.end())
    range_index.erase(range_it);
  m_plan.insert(entry);
  //HT_DEBUG_OUT << "Inserted range " << table.id << "[" << range.start_row << ".."
  //    << range.end_row << "] to location " << location << HT_END;
}

bool RangeServerRecoveryReceiverPlan::get_location(const TableIdentifier &table,
    const char *row, String &location) const {
  QualifiedRangeStateSpec state_spec(table, RangeSpec("",row));
  const RangeIndex &range_index = m_plan.get<ByRange>();
  RangeIndex::const_iterator range_it = range_index.lower_bound(state_spec);
  bool found = false;

  while(range_it != range_index.end() && table == range_it->state_spec.qualified_range.table) {
    if (strcmp(row, range_it->state_spec.qualified_range.range.start_row) > 0 &&
        strcmp(row, range_it->state_spec.qualified_range.range.end_row) <= 0 ) {
      location = range_it->location;
      found = true;
      break;
    }
    ++range_it;
  }
  return found;
}

void RangeServerRecoveryReceiverPlan::get_locations(StringSet &locations) const {
  const LocationIndex &location_index = m_plan.get<ByLocation>();
  LocationIndex::const_iterator location_it = location_index.begin();
  String last_location;
  for(; location_it != location_index.end(); ++location_it) {
    if (location_it->location != last_location) {
      last_location = location_it->location;
      locations.insert(last_location);
    }
  }
}

void RangeServerRecoveryReceiverPlan::get_ranges(vector<QualifiedRangeStateSpecManaged> &ranges) const {
  const RangeIndex &range_index = m_plan.get<ByRange>();
  RangeIndex::const_iterator range_it = range_index.begin();

  for(; range_it != range_index.end(); ++range_it)
    ranges.push_back(range_it->state_spec);
  HT_ASSERT(ranges.size());
}

void RangeServerRecoveryReceiverPlan::get_ranges(const char *location,
    vector<QualifiedRangeSpec> &ranges) {
  LocationIndex &location_index = m_plan.get<ByLocation>();
  pair<LocationIndex::iterator, LocationIndex::iterator> bounds =
      location_index.equal_range(location);
  LocationIndex::iterator location_it = bounds.first;
  for(; location_it != bounds.second; ++location_it) {
    ranges.push_back(location_it->state_spec.qualified_range);
  }
}

void RangeServerRecoveryReceiverPlan::get_ranges(const char *location,
    vector<QualifiedRangeStateSpec> &ranges) {
  LocationIndex &location_index = m_plan.get<ByLocation>();
  pair<LocationIndex::iterator, LocationIndex::iterator> bounds =
      location_index.equal_range(location);
  LocationIndex::iterator location_it = bounds.first;
  for(; location_it != bounds.second; ++location_it)
    ranges.push_back(location_it->state_spec);
}

void RangeServerRecoveryReceiverPlan::get_ranges(const char *location,
    vector<QualifiedRangeStateSpecManaged> &ranges) const {

  const LocationIndex &location_index = m_plan.get<ByLocation>();
  pair<LocationIndex::const_iterator, LocationIndex::const_iterator> bounds =
      location_index.equal_range(location);
  LocationIndex::const_iterator location_it = bounds.first;
  for(; location_it != bounds.second; ++location_it)
    ranges.push_back(location_it->state_spec);
}

bool RangeServerRecoveryReceiverPlan::get_range(const TableIdentifier &table, const char *row,
    QualifiedRangeStateSpec &range) {
  RangeIndex &range_index = m_plan.get<ByRange>();
  QualifiedRangeStateSpec target(table, RangeSpec("",row));
  RangeIndex::iterator range_it = range_index.lower_bound(target);

  bool found = false;

  while(range_it != range_index.end() && range_it->state_spec.qualified_range.table == table) {
    if (strcmp(row, range_it->state_spec.qualified_range.range.start_row) > 0 &&
        strcmp(row, range_it->state_spec.qualified_range.range.end_row) <= 0 ) {
      range = range_it->state_spec;
      found = true;
      break;
    }
    else if (strcmp(row, range_it->state_spec.qualified_range.range.start_row)<=0) {
      // gone too far
      break;
    }
    ++range_it;
  }
  //if (!found) {
  //  range_it = range_index.begin();
  //  HT_DEBUG_OUT << " Range not found " << table.id << " " << row
  //      << " existing ranges are..." << HT_END;
  //  while(range_it != range_index.end()) {
  //    HT_DEBUG_OUT << range_it->state_spec << HT_END;
  //    ++range_it;
  //  }
  //}

  return found;
}

bool RangeServerRecoveryReceiverPlan::get_range(const TableIdentifier &table, const char *row,
    QualifiedRangeSpec &range) {
  QualifiedRangeStateSpec target;
  bool retval = get_range(table, row, target);
  if (retval)
    range = target.qualified_range;
  return retval;
}

void RangeServerRecoveryReceiverPlan::erase_range(const QualifiedRangeSpec &range) {
  QualifiedRangeStateSpec state_spec(range);
  RangeIndex &range_index = m_plan.get<ByRange>();
  range_index.erase(state_spec);
}

size_t RangeServerRecoveryReceiverPlan::encoded_length() const {
  const LocationIndex &location_index = m_plan.get<ByLocation>();
  LocationIndex::const_iterator location_it = location_index.begin();
  size_t len = 4;

  while(location_it != location_index.end()) {
    len += location_it->encoded_length();
    ++location_it;
  }
  return len;
}

void RangeServerRecoveryReceiverPlan::encode(uint8_t **bufp) const {
  const LocationIndex &location_index = m_plan.get<ByLocation>();
  LocationIndex::const_iterator location_it = location_index.begin();

  Serialization::encode_i32(bufp, location_index.size());

  while(location_it != location_index.end()) {
    location_it->encode(bufp);
    ++location_it;
  }
}

void RangeServerRecoveryReceiverPlan::decode(const uint8_t **bufp, size_t *remainp) {
  size_t num_entries = Serialization::decode_i32(bufp, remainp);
  for(size_t ii=0; ii<num_entries; ++ii) {
    ReceiverEntry tmp_entry;
    tmp_entry.decode(bufp, remainp);
    ReceiverEntry entry(m_arena, tmp_entry.location, tmp_entry.state_spec.qualified_range.table,
        tmp_entry.state_spec.qualified_range.range, tmp_entry.state_spec.state);
    m_plan.insert(entry);
  }
}

size_t RangeServerRecoveryReceiverPlan::ReceiverEntry::encoded_length() const {
  return Serialization::encoded_length_vstr(location) + state_spec.encoded_length();
}

void RangeServerRecoveryReceiverPlan::ReceiverEntry::encode(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp,location);
  state_spec.encode(bufp);
}

void RangeServerRecoveryReceiverPlan::ReceiverEntry::decode(const uint8_t **bufp,
    size_t *remainp) {
  location = Serialization::decode_vstr(bufp, remainp);
  state_spec.decode(bufp,remainp);
}
