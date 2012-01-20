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

#include "Common/Compat.h"
#include "Common/Error.h"

#include "OperationRecoveryBlocker.h"
#include "GcWorker.h"

using namespace Hypertable;

OperationRecoveryBlocker::OperationRecoveryBlocker(ContextPtr &context)
  : Operation(context, MetaLog::EntityType::OPERATION_RECOVERY_BLOCKER) {
  m_obstructions.insert(Dependency::RECOVERY_BLOCKER);
}


void OperationRecoveryBlocker::execute() {
  HT_INFOF("Entering RecoveryBlocker-%lld", (Lld)header.id);
  size_t total_servers, connected_servers, quorum;
  try {
    try_again:
    total_servers = m_context->server_count();
    connected_servers = m_context->connected_server_count();
    quorum = (total_servers * m_context->props->get_i32("Hypertable.Failover.Quorum.Percentage") )/100;
    HT_INFO_OUT << "total_servers=" << total_servers << " connected_servers="
        << connected_servers << " quorum=" << quorum << HT_END;
    if (connected_servers < quorum || connected_servers == 0) {
      block();
    }
    else {
      while(1) {
        poll(0,0, 15000);
        size_t new_connected_servers = m_context->connected_server_count();
        if (new_connected_servers < connected_servers)
          goto try_again;
        if (new_connected_servers == connected_servers)
          break;
      }
      complete_ok();
    }

  }
  catch (Exception &e) {
    HT_THROW2(e.code(), e, "Recovery Blocker");
  }
  HT_INFOF("Leaving RecoveryBlocker-%lld state=%s", (Lld)header.id,
      OperationState::get_text(get_state()));
}

const String OperationRecoveryBlocker::name() {
  return "OperationRecoveryBlocker";
}

const String OperationRecoveryBlocker::label() {
  return "RecoveryBlocker";
}

