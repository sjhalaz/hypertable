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

#ifndef HYPERTABLE_RANGESERVERRECOVERYPLAN_H
#define HYPERTABLE_RANGESERVERRECOVERYPLAN_H

#include "Common/Compat.h"
#include <vector>
#include <map>

#include "Common/String.h"
#include "Common/Serialization.h"
#include "Types.h"
#include "RangeServerRecoveryLoadPlan.h"
#include "RangeServerRecoveryReplayPlan.h"

namespace Hypertable {

  class RangeServerRecoveryPlan {
  public:
    RangeServerRecoveryPlan() : type(RangeSpec::UNKNOWN) { }

    size_t encoded_length() const {
      return 4 + replay_plan.encoded_length() + load_plan.encoded_length();
    }

    void encode(uint8_t **bufp) const {
      Serialization::encode_i32(bufp, type);
      replay_plan.encode(bufp);
      load_plan.encode(bufp);
    }

    void decode(const uint8_t **bufp, size_t *remainp) {
      type = Serialization::decode_i32(bufp, remainp);
      replay_plan.decode(bufp, remainp);
      load_plan.decode(bufp, remainp);
    }

    friend std::ostream &operator<<(std::ostream &os, const RangeServerRecoveryPlan &plan) {
      os << "{RangeServerRecoveryPlan: type=" << plan.type << ", replay_plan="
         << plan.replay_plan << ", load_plan=" << plan.load_plan << "}";
      return os;
    }

    int type;
    RangeServerRecoveryReplayPlan replay_plan;
    RangeServerRecoveryLoadPlan load_plan;
  };

} // namespace Hypertable


#endif // HYPERTABLE_RANGESERVERRECOVERYPLAN_H
