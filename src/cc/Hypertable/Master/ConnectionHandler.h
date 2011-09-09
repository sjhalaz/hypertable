/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
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

#ifndef HYPERTABLE_CONNECTIONHANDLER_H
#define HYPERTABLE_CONNECTIONHANDLER_H

#include "AsyncComm/DispatchHandler.h"

#include "Operation.h"
#include "Context.h"

namespace Hypertable {

  /**
   */
  class ConnectionHandler : public DispatchHandler {
  public:
    ConnectionHandler(ContextPtr &context);
    virtual void handle(EventPtr &event);

  private:
    int32_t send_id_response(EventPtr &event, OperationPtr &operation);
    int32_t send_error_response(EventPtr &event, int32_t error, const String &msg);
    int32_t send_ok_response(EventPtr &event);
    ContextPtr  m_context;
    bool m_shutdown;
  };

}

#endif // HYPERTABLE_CONNECTIONHANDLER_H

