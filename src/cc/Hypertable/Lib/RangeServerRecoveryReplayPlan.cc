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

#include "Common/Serialization.h"

#include "RangeServerRecoveryReplayPlan.h"

using namespace std;
using namespace boost::multi_index;
using namespace Hypertable;

void RangeServerRecoveryReplayPlan::insert(const char *location, uint32_t fragment) {
  ReplayEntry entry(location, fragment);
  FragmentIndex &fragment_index = m_plan.get<ByFragment>();
  FragmentIndex::iterator fragment_it = fragment_index.find(entry.fragment);

  if(fragment_it != fragment_index.end())
    m_plan.erase(fragment_it);
  m_plan.insert(entry);
}

void RangeServerRecoveryReplayPlan::get_fragments(vector<uint32_t> &fragments) const {
  const FragmentIndex &fragment_index = m_plan.get<ByFragment>();
  FragmentIndex::const_iterator fragment_it = fragment_index.begin();

  for(; fragment_it != fragment_index.end(); ++fragment_it)
    fragments.push_back(fragment_it->fragment);
}


void RangeServerRecoveryReplayPlan::get_fragments(const char *location,
    vector<uint32_t> &fragments) const {
  const LocationIndex &location_index = m_plan.get<ByLocation>();
  pair<LocationIndex::const_iterator, LocationIndex::const_iterator> bounds =
      location_index.equal_range(location);
  LocationIndex::const_iterator location_it = bounds.first;
  for(; location_it != bounds.second; ++location_it)
    fragments.push_back(location_it->fragment);
}

void RangeServerRecoveryReplayPlan::get_locations(StringSet &locations) const {
  const LocationIndex &location_index = m_plan.get<ByLocation>();
  LocationIndex::const_iterator location_it = location_index.begin();
  String last_location;
  while(location_it != location_index.end()) {
    if (location_it->location != last_location) {
      last_location = location_it->location;
      locations.insert(last_location);
    }
    ++location_it;
  }
}

bool RangeServerRecoveryReplayPlan::get_location(uint32_t fragment, String &location) const {
  const FragmentIndex &fragment_index = m_plan.get<ByFragment>();
  FragmentIndex::iterator fragment_it = fragment_index.find(fragment);
  bool found = false;
  if(fragment_it != fragment_index.end()) {
    location = fragment_it->location;
    found = true;
  }
  return found;
}

size_t RangeServerRecoveryReplayPlan::encoded_length() const {
  size_t len = 4;
  const LocationIndex &location_index = m_plan.get<ByLocation>();
  LocationIndex::const_iterator location_it = location_index.begin();
  for(; location_it != location_index.end(); ++location_it)
    len += location_it->encoded_length();
  return len;
}

void RangeServerRecoveryReplayPlan::encode(uint8_t **bufp) const {
  const LocationIndex &location_index = m_plan.get<ByLocation>();
  LocationIndex::const_iterator location_it = location_index.begin();
  Serialization::encode_i32(bufp, location_index.size());
  for(; location_it != location_index.end(); ++location_it)
    location_it->encode(bufp);
}

void RangeServerRecoveryReplayPlan::decode(const uint8_t **bufp, size_t *remainp) {
  size_t num_entries = Serialization::decode_i32(bufp, remainp);
  for(size_t ii=0; ii<num_entries; ++ii) {
    ReplayEntry entry;
    entry.decode(bufp, remainp);
    m_plan.insert(entry);
  }
}

size_t RangeServerRecoveryReplayPlan::ReplayEntry::encoded_length() const {
  return Serialization::encoded_length_vstr(location) + 4;
}

void RangeServerRecoveryReplayPlan::ReplayEntry::encode(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp,location);
  Serialization::encode_i32(bufp,fragment);
}

void RangeServerRecoveryReplayPlan::ReplayEntry::decode(const uint8_t **bufp, size_t *remainp) {
  location = Serialization::decode_vstr(bufp, remainp);
  fragment = Serialization::decode_i32(bufp, remainp);
}



