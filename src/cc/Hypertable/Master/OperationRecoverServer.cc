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
#include "Common/Error.h"
#include "Common/md5.h"

#include "OperationRecoverServer.h"

using namespace Hypertable;

OperationRecoverServer::OperationRecoverServer(ContextPtr &context, RangeServerConnectionPtr &rsc)
  : Operation(context, MetaLog::EntityType::OPERATION_RECOVER_SERVER), m_rsc(rsc),
  m_hyperspace_handle(0), m_range_flags(0) {
  m_exclusivities.insert(m_rsc->location());
  m_obstructions.insert(Dependency::RECOVER_SERVER);
  m_hash_code = md5_hash("RecoverServer") ^ md5_hash(m_rsc->location().c_str());
}


void OperationRecoverServer::execute() {

  int state = get_state();
  if (!m_hyperspace_handle) {
    try {
      // need to wait for long enough to be certain that the RS has failed
      // before trying to acquire lock
      if (state == OperationState::INITIAL)
        wait_for_server();
      acquire_server_lock();
    }
    catch (Exception &e) {
      if (state != OperationState::INITIAL) {
        // this should never happen, ie no one else shd lock the Hyperspace file
        // after the OperationRecoverServer has started
        HT_THROW(e.code(), e.what());
      }
      else {
        if (m_rsc->connected()) {
          // range server temporarily disconnected but is back online
          HT_INFO_OUT << e << HT_END;
          complete_ok();
          return;
        }
        else {
          // range server is connected to Hyperspace but not to master
          HT_FATAL_OUT << e << HT_END;
        }
      }
    }
  }

  HT_INFOF("Entering RecoverServer %s state=%s",
           m_rsc->location(), OperationState::get_text(state));

  switch (state) {
  case OperationState::INITIAL:
    // read rsml figure out what types of ranges lived on this
    // and populate the various vectors of ranges
    read_rsml();
    set_state(OperationState::ISSUE_REQUESTS);
    HT_MAYBE_FAIL("recover-server-INITIAL-a");
    m_context->mml_writer->record_state(this);
    m_rsc->set_removed(true);
    HT_MAYBE_FAIL("recover-server-INITIAL-b");
    break;

  case OperationState::ISSUE_REQUESTS:
    std::vector<Entity *> entities;
    Operation *sub_op;

    if (m_range_flags & FLAG_HAS_ROOT) {
      sub_op = new OperationRecoverServerRanges(m_context, m_rsc, RangeSpec::ROOT,
                                                m_root_range);
      {
        ScopedLock lock(m_mutex);
        m_dependencies.insert(Dependency::ROOT);
      }
      m_sub_ops.push_back(sub_op);
      entities.push_back(sub_op);
    }
    if (m_range_flags & FLAG_HAS_METADATA) {
      sub_op = new OperationRecoverServerRanges(m_context, m_rsc, RangeSpec::METADATA,
                                                m_metadata_ranges);
      {
        ScopedLock lock(m_mutex);
        m_dependencies.insert(Dependency::METADATA);
      }
      m_sub_ops.push_back(sub_op);
      entities.push_back(sub_op);
    }
    if (m_range_flags & FLAG_HAS_SYSTEM) {
      sub_op = new OperationRecoverServerRanges(m_context, m_rsc, RangeSpec::SYSTEM,
                                                m_system_ranges);
      {
        ScopedLock lock(m_mutex);
        m_dependencies.insert(Dependency::SYSTEM);
      }
      m_sub_ops.push_back(sub_op);
      entities.push_back(sub_op);
    }
    if (m_range_flags & FLAG_HAS_USER) {
      sub_op = new OperationRecoverServerRanges(m_context, m_rsc, RangeSpec::USER,
                                                m_user_ranges);
      {
        ScopedLock lock(m_mutex);
        m_dependencies.insert(format("%s-user", m_rsc->location()));
      }
      m_sub_ops.push_back(sub_op);
      entities.push_back(sub_op);
    }
    set_state(OperationState::FINALIZE);
    entities.push_back(this);
    m_context->mml_writer->record_state(entities);
    HT_MAYBE_FAIL("recover-server-ISSUE_REQUESTS");
    break;

  case OperationState::FINALIZE:
    //Once recovery is complete, the master blows away the RSML and CL for the
    //server being recovered then it unlocks the hyperspace file
    HT_MAYBE_FAIL("recover-server-FINALIZE-a");
    clear_server_state();
    complete_ok();
    HT_MAYBE_FAIL("recover-server-FINALIZE-b");
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
    break;
  }

  HT_INFOF("Leaving RecoverServer %s state=%s",
           m_rsc->location().c_str(), OperationState::get_text(get_state()));
}

void OperationRecoverServer::~OperationRecoverServer() {
}

bool OperationRecoverServer::wait_for_server() {
  uint32_t wait_interval = m_context->props->get_i32("Hypertable.Failover.GracePeriod");
  uint32_t retry_interval = wait_interval/10;

  for(int ii=0; ii<10; ++ii)
    if (m_rsc->connected())
      break;
  return;
}

void OperationRecoverServer::acquire_server_lock() {

  uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_LOCK;
  uint32_t lock_status = LOCK_STATUS_BUSY;
  uint32_t retry_interval = m_context->props->get_i32("Hypertable.Connection.Retry.Interval");
  LockSequencer sequencer;
  bool reported = false;
  int max_retries=10;
  int retry_count=0;

  m_context->rs_recover_handles[m_rsc->location()] =
    m_context->hyperspace->open(m_context->toplevel_dir + "/servers/" + m_rsc->location(),
                                oflags);
  m_hyperspace_handle = m_context->rs_recover_handles[m_rsc->location()];

  while (lock_status != LOCK_STATUS_GRANTED) {
    context->hyperspace->try_lock(m_hyperspace_handle, LOCK_MODE_EXCLUSIVE, &lock_status,
        &sequencer);
    if (lock_status != LOCK_STATUS_GRANTED) {
      if (!reported) {
        HT_INFO_OUT << "Couldn't obtain lock on '" << m_context->toplevel_dir
          << "/servers/"<< m_rsc->location() << "' due to conflict, "
          << "entering retry loop ..." << HT_END;
        reported = true;
      }
      if (retry_count > max_retries) {
        HT_THROW(Error::HYPERSPACE_LOCK_CONFLICT, (String)"Couldn't obtain lock on '" +
            m_context->toplevel_dir + "/servers/" + m_rsc->location() +
            "' due to conflict,  hit max_retries " + retry_count);
      }
      poll(0, 0, retry_interval);
      ++retry_count;
    }
  }

  HT_INFO_OUT << "Obtained lock on " << m_context->toplevel_dir << "/servers/"
              << m_rsc->location() << HT_END;

}

void OperationRecoverServer::display_state(std::ostream &os) {
  os << " location=" << m_rsc->location() << " ";
}

const String OperationRecoverServer::name() {
  return "OperationRecoverServer";
}

const String OperationRecoverServer::label() {
  return format("RecoverServer %s", m_rsc->location());
}

const String OperationRecoverServer::graphviz_label() {
  return format("RecoverServer %s", m_rsc->location());
}


void OperationRecoverServer::clear_server_state() {
  // write empty rsml
  MetaLog::DefinitionPtr rsml_definition
      = new MetaLog::DefinitionRangeServer(m_rsc->location().c_str());
  vector<MetaLog::EntityPtr> entities;
  MetaLog::WriterPtr = new MetaLog::Writer(m_context->dfs, rsml_definition,
      m_context->top_level_dir + "/log/" + rsml_definition->name(), entities);
  m_rsc->set_removed(false);
  // unlock hyperspace file
  Hyperspace::close_handle_ptr(m_context->hyperspace, &m_hyperspace_handle);
}

void OperationRecoverServer::read_rsml() {
  // move rsml and commit log to some recovered dir
  MetaLog::DefinitionPtr rsml_definition
      = new MetaLog::DefinitionRangeServer(m_rsc->location().c_str());
  MetaLog::ReaderPtr rsml_reader;
  MetaLog::EntityRange *range_entity;
  vector<MetaLog::EntityPtr> entities;

  try {
    rsml_reader = new MetaLog::Reader(m_context->dfs, rsml_definition,
        m_context->top_level_dir + "/log/" + rsml_definition->name());
    rsml_reader->get_entities(entities);
    foreach(MetaLog::EntityPtr &entity, entities) {
      if ((range_entity = dynamic_cast<MetaLog::EntityRange *>(entity.get())) != 0) {
        QualifiedRangeSpec rs;
        // skip phantom ranges, let whoever was recovering them deal with them
        if (range_entity->state.state != RangeState::PHANTOM) {
          rs.range = range_entity->spec;
          rs.table = range_entity->table;
          if (rs.is_root())
            m_root_range.push_back(rs);
          else if (rs.table.is_metadata())
            m_metadata_ranges.push_back(rs);
          else if (rs.table.is_system())
            m_system_ranges.push_back(rs);
          else
            m_user_ranges.push_back(rs);
        }
      }
    }
  }
  catch (Exception &e) {
    HT_FATAL_OUT << e << HT_END;
  }
}

// XXX: SJ TODO: implement decode/encode methods
void OperationRecoverServer::encoded_state_length() const {
}

void OperationRecoverServer::encode_state(uint8_t **bufp) const {
}

void OperationRecoverServer::decode_state(const uint8_t **bufp, size_t *remainp) {
  decode_request(bufp, remainp);
}

void OperationRecoverServer::decode_request(const uint8_t **bufp, size_t *remainp) {
}


