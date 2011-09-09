/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
#include "Common/Time.h"

#include "AsyncComm/CommHeader.h"

#include "MasterProtocol.h"

namespace Hypertable {
  using namespace Serialization;

  CommBuf *
  MasterProtocol::create_create_namespace_request(const String &name, int flags) {
    CommHeader header(COMMAND_CREATE_NAMESPACE);
    CommBuf *cbuf = new CommBuf(header, encoded_length_vstr(name) + 4);
    cbuf->append_vstr(name);
    cbuf->append_i32(flags);
    return cbuf;
  }

  CommBuf *
  MasterProtocol::create_drop_namespace_request(const String &name, bool if_exists) {
    CommHeader header(COMMAND_DROP_NAMESPACE);
    CommBuf *cbuf = new CommBuf(header, encoded_length_vstr(name)+1);
    cbuf->append_vstr(name);
    cbuf->append_bool(if_exists);
    return cbuf;
  }

  CommBuf *
  MasterProtocol::create_rename_table_request(const String &old_name, const String &new_name) {
    CommHeader header(COMMAND_RENAME_TABLE);
    CommBuf *cbuf = new CommBuf(header, encoded_length_vstr(old_name) +
                                        encoded_length_vstr(new_name));
    cbuf->append_vstr(old_name);
    cbuf->append_vstr(new_name);
    return cbuf;
  }

  CommBuf *
  MasterProtocol::create_create_table_request(const String &tablename,
                                              const String &schemastr) {
    CommHeader header(COMMAND_CREATE_TABLE);
    CommBuf *cbuf = new CommBuf(header, encoded_length_vstr(tablename)
        + encoded_length_vstr(schemastr));
    cbuf->append_vstr(tablename);
    cbuf->append_vstr(schemastr);
    return cbuf;
  }

  CommBuf *
  MasterProtocol::create_alter_table_request(const String &tablename,
      const String &schemastr) {
    CommHeader header(COMMAND_ALTER_TABLE);
    CommBuf *cbuf = new CommBuf(header, encoded_length_vstr(tablename)
        + encoded_length_vstr(schemastr));
    cbuf->append_vstr(tablename);
    cbuf->append_vstr(schemastr);
    return cbuf;
  }

  CommBuf *MasterProtocol::create_get_schema_request(const String &tablename) {
    CommHeader header(COMMAND_GET_SCHEMA);
    CommBuf *cbuf = new CommBuf(header, encoded_length_vstr(tablename));
    cbuf->append_vstr(tablename);
    return cbuf;
  }

  CommBuf *MasterProtocol::create_status_request() {
    CommHeader header(COMMAND_STATUS);
    CommBuf *cbuf = new CommBuf(header, 0);
    return cbuf;
  }

  CommBuf *
  MasterProtocol::create_register_server_request(const String &location,
                                                 uint16_t listen_port,
                                                 StatsSystem &system_stats) {
    int64_t now = get_ts64();
    CommHeader header(COMMAND_REGISTER_SERVER);
    CommBuf *cbuf = new CommBuf(header, encoded_length_vstr(location) + 2 + system_stats.encoded_length() + 8);
    cbuf->append_vstr(location);
    cbuf->append_i16(listen_port);
    system_stats.encode(cbuf->get_data_ptr_address());
    cbuf->append_i64(now);
    return cbuf;
  }

  CommBuf *
  MasterProtocol::create_move_range_request(const TableIdentifier *table,
      const RangeSpec &range, const String &transfer_log_dir,
      uint64_t soft_limit, bool split) {
    CommHeader header(COMMAND_MOVE_RANGE);
    CommBuf *cbuf = new CommBuf(header, table->encoded_length()
        + range.encoded_length() + encoded_length_vstr(transfer_log_dir) + 9);
    table->encode(cbuf->get_data_ptr_address());
    range.encode(cbuf->get_data_ptr_address());
    cbuf->append_vstr(transfer_log_dir);
    cbuf->append_i64(soft_limit);
    if (split)
      cbuf->append_byte(1);
    else
      cbuf->append_byte(0);
    return cbuf;
  }


  CommBuf *
  MasterProtocol::create_relinquish_acknowledge_request(const TableIdentifier *table,
                                                        const RangeSpec &range) {
    CommHeader header(COMMAND_RELINQUISH_ACKNOWLEDGE);
    CommBuf *cbuf = new CommBuf(header, table->encoded_length()
                                + range.encoded_length());
    table->encode(cbuf->get_data_ptr_address());
    range.encode(cbuf->get_data_ptr_address());
    return cbuf;
  }

  CommBuf *
  MasterProtocol::create_drop_table_request(const String &table_name,
                                            bool if_exists) {
    CommHeader header(COMMAND_DROP_TABLE);
    CommBuf *cbuf = new CommBuf(header, 1 + encoded_length_vstr(table_name));
    cbuf->append_bool(if_exists);
    cbuf->append_vstr(table_name);
    return cbuf;
  }

  CommBuf *MasterProtocol::create_fetch_result_request(int64_t id) {
    CommHeader header(COMMAND_FETCH_RESULT);
    CommBuf *cbuf = new CommBuf(header, 8);
    cbuf->append_i64(id);
    return cbuf;
  }

  CommBuf *MasterProtocol::create_shutdown_request() {
    CommHeader header(COMMAND_SHUTDOWN);
    CommBuf *cbuf = new CommBuf(header, 0);
    return cbuf;
  }

  CommBuf *MasterProtocol::create_balance_request(BalancePlan &plan) {
    CommHeader header(COMMAND_BALANCE);
    CommBuf *cbuf = new CommBuf(header, plan.encoded_length());
    plan.encode(cbuf->get_data_ptr_address());
    return cbuf;
  }

  CommBuf *MasterProtocol::create_player_complete_request(int64_t op_id, uint32_t attempt,
      const map<uint32_t, int> &error_map) {
    CommHeader header(COMMAND_PLAYER_COMPLETE);
    size_t len = Serialization::encoded_length_vi64(op_id) +
        Serialization::encoded_length_vi32(attempt) +
        Serialization::encoded_length_vi32(error_map.size());

    map<uint32_t, int>::const_iterator it = error_map.begin();
    while(it != error_map.end()) {
      len += Serialization::encoded_length_vi32(it->first) +
             Serialization::encoded_length_vi32(it->second);
      ++it;
    }
    CommBuf *cbuf = new CommBuf(header, len);
    Serialization::encode_vi64(cbuf->get_data_ptr_address(), op_id);
    Serialization::encode_vi32(cbuf->get_data_ptr_address(), attempt);
    Serialization::encode_vi32(cbuf->get_data_ptr_address(), error_map.size());
    it = error_map.begin();
    while(it != error_map.end()) {
      Serialization::encode_vi32(it->first);
      Serialization::encode_vi32(it->second);
      ++it;
    }
    return cbuf;
  }

  CommBuf *MasterProtocol::create_phantom_prepare_complete_request(int64_t op_id,
      uint32_t attempt, const map<QualifiedRangeSpec, int> &error_map) {
    CommHeader header(COMMAND_PHANTOM_PREPARE_COMPLETE);
    size_t len = Serialization::encoded_length_vi64(op_id) +
        Serialization::encoded_length_vi32(attempt) +
        Serialization::encoded_length_vi32(error_map.size());

    map<QualifiedRangeSpec, int>::const_iterator it = error_map.begin();
    while(it != error_map.end()) {
      len += it->first.encoded_length() + Serialization::encoded_length_vi32(it->second);
      ++it;
    }
    CommBuf *cbuf = new CommBuf(header, len);
    Serialization::encode_vi64(cbuf->get_data_ptr_address(), op_id);
    Serialization::encode_vi32(cbuf->get_data_ptr_address(), attempt);
    Serialization::encode_vi32(cbuf->get_data_ptr_address(), error_map.size());
    it = error_map.begin();
    while(it != error_map.end()) {
      it->first.encode(cbuf->get_data_ptr_address());
      Serialization::encode_vi32(it->second);
      ++it;
    }
    return cbuf;
  }

  CommBuf *MasterProtocol::create_phantom_commit_complete_request(int64_t op_id,
      uint32_t attempt, const map<QualifiedRangeSpec, int> &error_map) {
    // XXX: TODO same as phantom prepare complete, consolidate code
    CommHeader header(COMMAND_PHANTOM_COMMIT_COMPLETE);
    size_t len = Serialization::encoded_length_vi64(op_id) +
        Serialization::encoded_length_vi32(attempt) +
        Serialization::encoded_length_vi32(error_map.size());

    map<QualifiedRangeSpec, int>::const_iterator it = error_map.begin();
    while(it != error_map.end()) {
      len += it->first.encoded_length() + Serialization::encoded_length_vi32(it->second);
      ++it;
    }
    CommBuf *cbuf = new CommBuf(header, len);
    Serialization::encode_vi64(cbuf->get_data_ptr_address(), op_id);
    Serialization::encode_vi32(cbuf->get_data_ptr_address(), attempt);
    Serialization::encode_vi32(cbuf->get_data_ptr_address(), error_map.size());
    it = error_map.begin();
    while(it != error_map.end()) {
      it->first.encode(cbuf->get_data_ptr_address());
      Serialization::encode_vi32(it->second);
      ++it;
    }
    return cbuf;
  }

  const char *MasterProtocol::m_command_strings[] = {
    "create table",
    "get schema",
    "status",
    "register server",
    "report split",
    "drop table",
    "alter table",
    "shutdown",
    "close",
    "create namespace",
    "drop namespace",
    "rename table",
    "relinquish acknowledge",
    "fetch result",
    "balance",
    "player complete",
    "phantom prepare complete",
    "phantom commit complete"
  };

  const char *MasterProtocol::command_text(uint64_t command) {
    if (command < 0 || command >= COMMAND_MAX)
      return "UNKNOWN";
    return m_command_strings[command];
  }

}

