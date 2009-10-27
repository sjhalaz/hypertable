/**
 * Copyright (C) 2009 Sanjit Jhala(Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
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

#ifndef HYPERSPACE_REQUESTHANDLERRECOVEROPEN_H
#define HYPERSPACE_REQUESTHANDLERRECOVEROPEN_H

#include "Common/Runnable.h"

#include "AsyncComm/ApplicationHandler.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/Event.h"


namespace Hyperspace {

  class Master;

  class RequestHandlerRecoverOpen : public ApplicationHandler {
  public:
    RequestHandlerRecoverOpen(Master *master, uint64_t session_id, uint64_t req_id)
      : ApplicationHandler(0), m_master(master), m_session_id(session_id), m_req_id(req_id) { }

    virtual void run();

  private:
    Master      *m_master;
    uint64_t     m_session_id;
    uint64_t     m_req_id;
  };
}

#endif // HYPERSPACE_REQUESTHANDLERRECOVEROPEN_H
