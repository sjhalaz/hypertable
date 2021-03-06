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

#ifndef HYPERTABLE_OPERATIONRECOVERSERVER_H
#define HYPERTABLE_OPERATIONRECOVERSERVER_H

#include <vector>
#include "Lib/Types.h"

#include "Operation.h"
#include "RangeServerConnection.h"


namespace Hypertable {

  using namespace std;

  class OperationRecoverServer : public Operation {
  public:
    OperationRecoverServer(ContextPtr &context, RangeServerConnectionPtr &rsc);
    virtual ~OperationRecoverServer();

    virtual void execute();
    virtual const String name();
    virtual const String label();
    virtual void display_state(std::ostream &os);
    virtual size_t encoded_state_length() const;
    virtual void encode_state(uint8_t **bufp) const;
    virtual void decode_state(const uint8_t **bufp, size_t *remainp);
    virtual void decode_request(const uint8_t **bufp, size_t *remainp);

  private:
    enum {
      FLAG_HAS_ROOT     = 0x0001,
      FLAG_HAS_METADATA = 0x0002,
      FLAG_HAS_SYSTEM   = 0x0004,
      FLAG_HAS_USER     = 0x0008
    };

    // acquire lock on Hyperspace file
    void acquire_server_lock();
    void read_rsml();
    void clear_server_state();
    // returns true if server connected
    void wait_for_server();

    vector<QualifiedRangeSpecManaged> m_root_range;
    vector<QualifiedRangeSpecManaged> m_metadata_ranges;
    vector<QualifiedRangeSpecManaged> m_system_ranges;
    vector<QualifiedRangeSpecManaged> m_user_ranges;

    RangeServerConnectionPtr m_rsc;
    uint64_t m_hyperspace_handle;
    int m_range_flags;

  };
  typedef intrusive_ptr<OperationRecoverServer> OperationRecoverServerPtr;

} // namespace Hypertable

#endif // HYPERTABLE_OPERATIONRECOVERSERVER_H
