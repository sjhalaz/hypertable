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

#ifndef HYPERTABLE_OPERATIONRECOVERSERVERRANGES_H
#define HYPERTABLE_OPERATIONRECOVERSERVERRANGES_H

#include <vector>

#include "Hypertable/Lib/Types.h"
#include "Hypertable/Lib/RangeServerRecoveryPlan.h"

#include "Operation.h"
#include "RangeServerConnection.h"


namespace Hypertable {

  using namespace std;

  class OperationRecoverServerRanges : public Operation {
  public:
    OperationRecoverServerRanges(ContextPtr &context, const String &location, int type,
        vector<QualifiedRangeStateSpecManaged> &ranges);
    OperationRecoverServerRanges(ContextPtr &context, const MetaLog::EntityHeader &header_);

    virtual void execute();
    virtual const String name();
    virtual const String label();
    virtual void display_state(std::ostream &os);
    virtual size_t encoded_state_length() const;
    virtual void encode_state(uint8_t **bufp) const;
    virtual void decode_state(const uint8_t **bufp, size_t *remainp);
    virtual void decode_request(const uint8_t **bufp, size_t *remainp);

  private:
    // make sure all recovery participants are available
    bool validate_recovery_plan();
    void initialize_obstructions_dependencies();
    void get_recovery_plan();
    bool replay_commit_log();
    bool prepare_to_commit();
    bool commit();
    bool acknowledge();
    void assign_ranges(const vector<QualifiedRangeStateSpecManaged> &ranges,
                       const StringSet &active_locations);
    void assign_ranges(const vector<QualifiedRangeStateSpec> &ranges,
                       const StringSet &active_locations);
    void assign_players(const vector<uint32_t> &fragments, const StringSet &active_locations);
    void read_fragment_ids();
    void set_type_str();

    String m_location;
    int m_type;
    uint32_t m_attempt;
    RangeServerRecoveryPlan m_plan;
    String m_type_str;
    vector<QualifiedRangeStateSpecManaged> m_ranges;
    vector<uint32_t> m_fragments;
    uint32_t m_timeout;
  };
  typedef intrusive_ptr<OperationRecoverServerRanges> OperationRecoverServerRangesPtr;

} // namespace Hypertable

#endif // HYPERTABLE_OPERATIONRECOVERSERVERRANGES_H
