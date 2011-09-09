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
#include "RangeServerRecoveryLoadPlan.h"

using namespace Hypertable;
using namespace boost::multi_index;
using namespace std;

void RangeServerRecoveryLoadPlan::insert(const char *location, const TableIdentifier &table,
    const RangeSpec &range) {
  LoadEntry entry(m_arena, location, table, range);
  RangeIndex &range_index = m_plan.get<ByRange>();
  RangeIndex::iterator range_it = range_index.find(entry.range);
  if (range_it != range_index.end())
    range_index.erase(range_it);
  m_plan.insert(entry);
}

bool RangeServerRecoveryLoadPlan::get_location(const TableIdentifier &table,
    const char *row, String &location) const {
  QualifiedRangeSpec range(table, RangeSpec(row,row));
  const RangeIndex &range_index = m_plan.get<ByRange>();
  RangeIndex::const_iterator range_it = range_index.lower_bound(range);
  bool found = false;

  while(table == range_it->range.table && range_it != range_index.end()) {
    if (strcmp(row, range_it->range.range.start_row) > 0 &&
        strcmp(row, range_it->range.range.end_row) <= 0 ) {
      location = range_it->location;
      found = true;
      break;
    }
    ++range_it;
  }
  return found;
}

void RangeServerRecoveryLoadPlan::get_locations(StringSet &locations) const {
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

void RangeServerRecoveryLoadPlan::get_ranges(vector<QualifiedRangeSpecManaged> &ranges) const {
  const RangeIndex &range_index = m_plan.get<ByRange>();
  RangeIndex::const_iterator range_it = range_index.begin();

  for(; range_it != range_index.end(); ++range_it)
    ranges.push_back(range_it->range);
}

void RangeServerRecoveryLoadPlan::get_ranges(const char *location,
    vector<QualifiedRangeSpec*> &ranges) {
  LocationIndex &location_index = m_plan.get<ByLocation>();
  pair<LocationIndex::const_iterator, LocationIndex::const_iterator> bounds =
      location_index.equal_range(location);
  LocationIndex::const_iterator location_it = bounds.first;
  for(; location_it != bounds.second; ++location_it)
    ranges.push_back(&(location_it->range));
}

void RangeServerRecoveryLoadPlan::get_ranges(const char *location,
    vector<QualifiedRangeSpec> &ranges) {
  LocationIndex &location_index = m_plan.get<ByLocation>();
  pair<LocationIndex::const_iterator, LocationIndex::const_iterator> bounds =
      location_index.equal_range(location);
  LocationIndex::const_iterator location_it = bounds.first;
  for(; location_it != bounds.second; ++location_it)
    ranges.push_back(location_it->range);
}

void RangeServerRecoveryLoadPlan::get_ranges(const char *location,
    vector<QualifiedRangeSpecManaged> &ranges) const {

  const LocationIndex &location_index = m_plan.get<ByLocation>();
  pair<LocationIndex::const_iterator, LocationIndex::const_iterator> bounds =
      location_index.equal_range(location);
  LocationIndex::const_iterator location_it = bounds.first;
  for(; location_it != bounds.second; ++location_it)
    ranges.push_back(location_it->range);
}

bool RangeServerRecoveryLoadPlan::get_range(const TableIdentifier &table, const char *row,
    QualifiedRangeSpec &range) {
  RangeIndex &range_index = m_plan.get<ByRange>();
  QualifiedRangeSpec target(table, RangeSpec(row,row));
  RangeIndex::iterator range_it = range_index.lower_bound(target);

  bool found = false;

  while(range_it->range.table == table && range_it != range_index.end()) {
    if (strcmp(row, range_it->range.range.start_row) > 0 &&
        strcmp(row, range_it->range.range.end_row) <= 0 ) {
      range = range_it->range;
      found = true;
      break;
    }
    else if (strcmp(row, range_it->range.range.start_row)<=0) {
      // gone too far
      break;
    }
  }
  return found;
}

void RangeServerRecoveryLoadPlan::erase_range(const QualifiedRangeSpec &range) {
  RangeIndex &range_index = m_plan.get<ByRange>();
  range_index.erase(range);
}

size_t RangeServerRecoveryLoadPlan::encoded_length() const {
  const LocationIndex &location_index = m_plan.get<ByLocation>();
  LocationIndex::const_iterator location_it = location_index.begin();
  size_t len = 4;

  while(location_it != location_index.end()) {
    len += location_it->encoded_length();
    ++location_it;
  }
  return len;
}

void RangeServerRecoveryLoadPlan::encode(uint8_t **bufp) const {
  const LocationIndex &location_index = m_plan.get<ByLocation>();
  LocationIndex::const_iterator location_it = location_index.begin();

  Serialization::encode_i32(bufp, location_index.size());

  while(location_it != location_index.end()) {
    location_it->encode(bufp);
    ++location_it;
  }
}

void RangeServerRecoveryLoadPlan::decode(const uint8_t **bufp, size_t *remainp) {
  size_t num_entries = Serialization::decode_i32(bufp, remainp);
  for(size_t ii=0; ii<num_entries; ++ii) {
    LoadEntry entry;
    entry.decode(bufp, remainp);
    m_plan.insert(entry);
  }
}

size_t RangeServerRecoveryLoadPlan::LoadEntry::encoded_length() const {
  return Serialization::encoded_length_vstr(location) + range.encoded_length();
}

void RangeServerRecoveryLoadPlan::LoadEntry::encode(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp,location);
  range.encode(bufp);
}

void RangeServerRecoveryLoadPlan::LoadEntry::decode(const uint8_t **bufp, size_t *remainp) {
  location = Serialization::decode_vstr(bufp, remainp);
  range.decode(bufp,remainp);
}
