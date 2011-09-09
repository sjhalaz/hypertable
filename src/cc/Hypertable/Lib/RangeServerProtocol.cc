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
#include "AsyncComm/CommBuf.h"
#include "AsyncComm/CommHeader.h"

#include "RangeServerProtocol.h"

namespace Hypertable {

  using namespace Serialization;

  const char *RangeServerProtocol::m_command_strings[] = {
    "load range",
    "update",
    "create scanner",
    "fetch scanblock",
    "compact",
    "status",
    "shutdown",
    "dump",
    "destroy scanner",
    "drop table",
    "drop range",
    "replay begin",
    "replay load range",
    "replay update",
    "replay commit",
    "get statistics",
    "update schema",
    "commit log sync",
    "close",
    "wait for maintenance",
    "acknowledge load",
    "relinquish range",
    "heapcheck",
    "metadata sync",
    "play fragments",
    "phantom load",
    "phantom update",
    "phantom prepare ranges",
    "phantom commit ranges",
    (const char *)0
  };

  const char *RangeServerProtocol::command_text(uint64_t command) {
    if (command < 0 || command >= COMMAND_MAX)
      return "UNKNOWN";
    return m_command_strings[command];
  }

  String
  RangeServerProtocol::compact_flags_to_string(uint32_t flags) {
    String str;
    if ((flags & COMPACT_FLAG_ALL) == COMPACT_FLAG_ALL)
      str += "ALL";
    else {
      bool first=true;
      if ((flags & COMPACT_FLAG_ROOT) == COMPACT_FLAG_ROOT) {
        str += "ROOT";
        first=false;
      }
      if ((flags & COMPACT_FLAG_METADATA) == COMPACT_FLAG_METADATA) {
        if (!first) {
          str += "|";
          first=false;
        }
        str += "METADATA";
      }
      if ((flags & COMPACT_FLAG_SYSTEM) == COMPACT_FLAG_SYSTEM) {
        if (!first) {
          str += "|";
          first=false;
        }
        str += "SYSTEM";
      }
      if ((flags & COMPACT_FLAG_USER) == COMPACT_FLAG_USER) {
        if (!first) {
          str += "|";
          first=false;
        }
        str += "USER";
      }
    }
    return str;
  }

  CommBuf *
  RangeServerProtocol::create_request_compact(const String &table_id, uint32_t flags) {
    CommHeader header(COMMAND_COMPACT);
    CommBuf *cbuf = new CommBuf(header, Serialization::encoded_length_vstr(table_id) + 4);
    Serialization::encode_vstr(cbuf->get_data_ptr_address(), table_id);
    Serialization::encode_i32(cbuf->get_data_ptr_address(), flags);
    return cbuf;
  }

  CommBuf *
  RangeServerProtocol::create_request_metadata_sync(const String &table_id, uint32_t flags,
                                                    std::vector<String> &columns) {
    CommHeader header(COMMAND_METADATA_SYNC);
    size_t length = Serialization::encoded_length_vstr(table_id) + 8;
    for (size_t i=0; i<columns.size(); i++)
      length += Serialization::encoded_length_vstr(columns[i]);
    CommBuf *cbuf = new CommBuf(header, length);
    Serialization::encode_vstr(cbuf->get_data_ptr_address(), table_id);
    Serialization::encode_i32(cbuf->get_data_ptr_address(), flags);
    Serialization::encode_i32(cbuf->get_data_ptr_address(), columns.size());
    for (size_t i=0; i<columns.size(); i++)
      Serialization::encode_vstr(cbuf->get_data_ptr_address(), columns[i]);
    return cbuf;
  }

  CommBuf *
  RangeServerProtocol::create_request_load_range(const TableIdentifier &table,
      const RangeSpec &range, const char *transfer_log,
      const RangeState &range_state, bool needs_compaction) {
    CommHeader header(COMMAND_LOAD_RANGE);
    header.flags |= CommHeader::FLAGS_BIT_URGENT;
    CommBuf *cbuf = new CommBuf(header, table.encoded_length()
        + range.encoded_length() + encoded_length_str16(transfer_log)
        + range_state.encoded_length() + 1);
    table.encode(cbuf->get_data_ptr_address());
    range.encode(cbuf->get_data_ptr_address());
    encode_str16(cbuf->get_data_ptr_address(), transfer_log);
    range_state.encode(cbuf->get_data_ptr_address());
    Serialization::encode_bool(cbuf->get_data_ptr_address(), needs_compaction);
    return cbuf;
  }

  CommBuf *
  RangeServerProtocol::create_request_update(const TableIdentifier &table,
      uint32_t count, StaticBuffer &buffer, uint32_t flags) {
    CommHeader header(COMMAND_UPDATE);
    if (table.is_system()) // If system table, set the urgent bit
      header.flags |= CommHeader::FLAGS_BIT_URGENT;
    CommBuf *cbuf = new CommBuf(header, 8 + table.encoded_length(), buffer);
    table.encode(cbuf->get_data_ptr_address());
    cbuf->append_i32(count);
    cbuf->append_i32(flags);
    return cbuf;
  }

  CommBuf *
  RangeServerProtocol::create_request_update_schema(
      const TableIdentifier &table, const String &schema) {
    CommHeader header(COMMAND_UPDATE_SCHEMA);
    if (table.is_system()) // If system table, set the urgent bit
      header.flags |= CommHeader::FLAGS_BIT_URGENT;
    CommBuf *cbuf = new CommBuf(header, table.encoded_length()
        + encoded_length_vstr(schema));
    table.encode(cbuf->get_data_ptr_address());
    cbuf->append_vstr(schema);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_commit_log_sync(const TableIdentifier &table) {
    CommHeader header(COMMAND_COMMIT_LOG_SYNC);
    if (table.is_system()) // If system table, set the urgent bit
      header.flags |= CommHeader::FLAGS_BIT_URGENT;
    CommBuf *cbuf = new CommBuf(header, table.encoded_length());
    table.encode(cbuf->get_data_ptr_address());
    return cbuf;
  }

  CommBuf *RangeServerProtocol::
  create_request_create_scanner(const TableIdentifier &table,
      const RangeSpec &range, const ScanSpec &scan_spec) {
    CommHeader header(COMMAND_CREATE_SCANNER);
    if (table.is_system()) // If system table, set the urgent bit
      header.flags |= CommHeader::FLAGS_BIT_URGENT;
    CommBuf *cbuf = new CommBuf(header, table.encoded_length()
        + range.encoded_length() + scan_spec.encoded_length());
    table.encode(cbuf->get_data_ptr_address());
    range.encode(cbuf->get_data_ptr_address());
    scan_spec.encode(cbuf->get_data_ptr_address());
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_destroy_scanner(int scanner_id) {
    CommHeader header(COMMAND_DESTROY_SCANNER);
    header.gid = scanner_id;
    CommBuf *cbuf = new CommBuf(header, 4);
    cbuf->append_i32(scanner_id);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_fetch_scanblock(int scanner_id) {
    CommHeader header(COMMAND_FETCH_SCANBLOCK);
    header.gid = scanner_id;
    CommBuf *cbuf = new CommBuf(header, 4);
    cbuf->append_i32(scanner_id);
    return cbuf;
  }

  CommBuf *
  RangeServerProtocol::create_request_drop_table(const TableIdentifier &table) {
    CommHeader header(COMMAND_DROP_TABLE);
    CommBuf *cbuf = new CommBuf(header, table.encoded_length());
    table.encode(cbuf->get_data_ptr_address());
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_status() {
    CommHeader header(COMMAND_STATUS);
    header.flags |= CommHeader::FLAGS_BIT_URGENT;
    CommBuf *cbuf = new CommBuf(header);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_close() {
    CommHeader header(COMMAND_CLOSE);
    header.flags |= CommHeader::FLAGS_BIT_URGENT;
    CommBuf *cbuf = new CommBuf(header);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_wait_for_maintenance() {
    CommHeader header(COMMAND_WAIT_FOR_MAINTENANCE);
    header.flags |= CommHeader::FLAGS_BIT_URGENT;
    CommBuf *cbuf = new CommBuf(header);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_shutdown() {
    CommHeader header(COMMAND_SHUTDOWN);
    header.flags |= CommHeader::FLAGS_BIT_URGENT;
    CommBuf *cbuf = new CommBuf(header);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_dump(const String &outfile,
						    bool nokeys) {
    CommHeader header(COMMAND_DUMP);
    header.flags |= CommHeader::FLAGS_BIT_URGENT;
    CommBuf *cbuf =
      new CommBuf(header, Serialization::encoded_length_vstr(outfile)
		  + 1);
    Serialization::encode_vstr(cbuf->get_data_ptr_address(), outfile.c_str());
    Serialization::encode_bool(cbuf->get_data_ptr_address(), nokeys);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_replay_begin(uint16_t group) {
    CommHeader header(COMMAND_REPLAY_BEGIN);
    CommBuf *cbuf = new CommBuf(header, 2);
    cbuf->append_i16(group);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::
  create_request_replay_load_range(const TableIdentifier &table,
      const RangeSpec &range, const RangeState &range_state) {
    CommHeader header(COMMAND_REPLAY_LOAD_RANGE);
    CommBuf *cbuf = new CommBuf(header, table.encoded_length()
        + range.encoded_length() + range_state.encoded_length());
    table.encode(cbuf->get_data_ptr_address());
    range.encode(cbuf->get_data_ptr_address());
    range_state.encode(cbuf->get_data_ptr_address());
    return cbuf;
  }

  CommBuf *
  RangeServerProtocol::create_request_replay_update(StaticBuffer &buffer) {
    CommHeader header(COMMAND_REPLAY_UPDATE);
    CommBuf *cbuf = new CommBuf(header, 0, buffer);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_replay_commit() {
    CommHeader header(COMMAND_REPLAY_COMMIT);
    CommBuf *cbuf = new CommBuf(header);
    return cbuf;
  }

  CommBuf *
  RangeServerProtocol::create_request_drop_range(const TableIdentifier &table,
                                                 const RangeSpec &range) {
    CommHeader header(COMMAND_DROP_RANGE);
    CommBuf *cbuf = new CommBuf(header, table.encoded_length()
                                + range.encoded_length());
    table.encode(cbuf->get_data_ptr_address());
    range.encode(cbuf->get_data_ptr_address());
    return cbuf;
  }

  CommBuf *
  RangeServerProtocol::create_request_acknowledge_load(const vector<QualifiedRangeSpec*> &ranges) {
    CommHeader header(COMMAND_ACKNOWLEDGE_LOAD);
    size_t len=4;
    foreach(const QualifiedRangeSpec *range, ranges)
      len += range->encoded_length();

    CommBuf *cbuf = new CommBuf(header, len);
    Serialization::encode_i32(cbuf->get_data_ptr_address(), ranges.size());
    foreach(const QualifiedRangeSpec *range, ranges)
      range->encode(cbuf->get_data_ptr_address());

    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_get_statistics() {
    CommHeader header(COMMAND_GET_STATISTICS);
    header.flags |= CommHeader::FLAGS_BIT_URGENT;
    CommBuf *cbuf = new CommBuf(header);
    return cbuf;
  }

  CommBuf *
  RangeServerProtocol::create_request_relinquish_range(const TableIdentifier &table,
                                                 const RangeSpec &range) {
    CommHeader header(COMMAND_RELINQUISH_RANGE);
    CommBuf *cbuf = new CommBuf(header, table.encoded_length()
                                + range.encoded_length());
    table.encode(cbuf->get_data_ptr_address());
    range.encode(cbuf->get_data_ptr_address());
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_heapcheck(const String &outfile) {
    CommHeader header(COMMAND_HEAPCHECK);
    header.flags |= CommHeader::FLAGS_BIT_URGENT;
    CommBuf *cbuf = new CommBuf(header, Serialization::encoded_length_vstr(outfile));
    Serialization::encode_vstr(cbuf->get_data_ptr_address(), outfile.c_str());
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_play_fragments(int64_t op_id, uint32_t attempt,
      const String &recover_location, int type, const vector<uint32_t> &fragments,
      const RangeServerRecoveryLoadPlan &load_plan, uint32_t replay_timeout) {
    CommHeader header(COMMAND_PLAY_FRAGMENTS);
    header.flags |= CommHeader::FLAGS_BIT_URGENT;
    size_t len = Serialization::encoded_length_vi64(op_id);
    len += Serialization::encoded_length_vi32(attempt);
    len +=Serialization::encoded_length_vstr(recover_location.c_str());
    len += Serialization::encoded_length_vi32(type);
    len += 4;
    for(size_t ii=0; ii<fragments.size(); ++ii)
      len += Serialization::encoded_length_vi32(fragments[ii]);
    len += load_plan.encoded_length();
    len += 4;

    CommBuf *cbuf = new CommBuf(header, len);
    Serialization::encode_vi64(cbuf->get_data_ptr_address(), op_id);
    Serialization::encode_vi32(cbuf->get_data_ptr_address(), attempt);
    Serialization::encode_vstr(cbuf->get_data_ptr_address(), recover_location.c_str());
    Serialization::encode_vi32(cbuf->get_data_ptr_address(), type);
    Serialization::encode_i32(cbuf->get_data_ptr_address(), fragments.size());
    for(size_t ii=0; ii<fragments.size(); ++ii)
      Serialization::encode_vi32(cbuf->get_data_ptr_address(), fragments[ii]);
    load_plan.encode(cbuf->get_data_ptr_address());
    Serialization::encode_i32(cbuf->get_data_ptr_address(), replay_timeout);

    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_phantom_load(const String &location,
      const vector<uint32_t> &fragments, const vector<QualifiedRangeSpec> &ranges) {
    CommHeader header(COMMAND_PHANTOM_LOAD);
    header.flags |= CommHeader::FLAGS_BIT_URGENT;
    size_t len = Serialization::encoded_length_vstr(location.c_str());
    len += 4;
    for(size_t ii=0; ii<fragments.size(); ++ii)
      len += Serialization::encoded_length_vi32(fragments[ii]);
    len += 4;
    for(size_t ii=0; ii<ranges.size(); ++ii)
      len += ranges[ii].encoded_length();

    CommBuf *cbuf = new CommBuf(header, len);
    Serialization::encode_vstr(cbuf->get_data_ptr_address(), location.c_str());
    Serialization::encode_i32(cbuf->get_data_ptr_address(), fragments.size());
    for(size_t ii=0; ii<fragments.size(); ++ii)
      Serialization::encode_vi32(cbuf->get_data_ptr_address(), fragments[ii]);
    Serialization::encode_i32(cbuf->get_data_ptr_address(), ranges.size());
    for(size_t ii=0; ii<ranges.size(); ++ii)
      ranges[ii].encode(cbuf->get_data_ptr_address());

    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_phantom_update(const QualifiedRangeSpec &range,
      const String &location, uint32_t fragment, bool more, StaticBuffer &buffer) {
    CommHeader header(COMMAND_PHANTOM_UPDATE);
    header.flags |= CommHeader::FLAGS_BIT_URGENT;
    size_t len = Serialization::encoded_length_vstr(location) + range.encoded_length() +
                 Serialization::encoded_length_vi32(fragment) + 1;
    CommBuf *cbuf = new CommBuf(header, len, buffer);
    Serialization::encoded_vstr(cbuf->get_data_ptr_address(), location);
    range.encode(cbuf->get_data_ptr_address());
    Serialization::encode_vi32(cbuf->get_data_ptr_address(), fragment);
    Serialization::encode_bool(cbuf->get_data_ptr_address(), more);
    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_phantom_prepare_ranges(int64_t op_id,
      uint32_t attempt, const String &location, const vector<QualifiedRangeSpec> &ranges,
      uint32_t timeout_ms) {
    CommHeader header(COMMAND_PHANTOM_PREPARE_RANGES);
    header.flags |= CommHeader::FLAGS_BIT_URGENT;
    size_t len = Serialization::encoded_length_vi64(op_id);
    len += Serialization::encoded_length_vi32(attempt);
    len +=Serialization::encoded_length_vstr(location.c_str());
    len += 4;
    for(size_t ii=0; ii<ranges.size(); ++ii)
      len += ranges[ii].encoded_length();
    len += 4;

    CommBuf *cbuf = new CommBuf(header, len);
    Serialization::encode_vi64(cbuf->get_data_ptr_address(), op_id);
    Serialization::encode_vi32(cbuf->get_data_ptr_address(), attempt);
    Serialization::encode_vstr(cbuf->get_data_ptr_address(), location.c_str());
    Serialization::encode_i32(cbuf->get_data_ptr_address(), ranges.size());
    for(size_t ii=0; ii<ranges.size(); ++ii)
      ranges[ii].encode(cbuf->get_data_ptr_address());
    Serialization::encode_i32(cbuf->get_data_ptr_address(), timeout_ms);

    return cbuf;
  }

  CommBuf *RangeServerProtocol::create_request_phantom_commit_ranges(int64_t op_id,
      uint32_t attempt, const String &location, const vector<QualifiedRangeSpec> &ranges,
      uint32_t timeout_ms) {
    CommHeader header(COMMAND_PHANTOM_COMMIT_RANGES);
    header.flags |= CommHeader::FLAGS_BIT_URGENT;
    size_t len = Serialization::encoded_length_vi64(op_id);
    len += Serialization::encoded_length_vi32(attempt);
    len +=Serialization::encoded_length_vstr(location.c_str());
    len += 4;
    for(size_t ii=0; ii<ranges.size(); ++ii)
      len += ranges[ii].encoded_length();
    len += 4;

    CommBuf *cbuf = new CommBuf(header, len);
    Serialization::encode_vi64(cbuf->get_data_ptr_address(), op_id);
    Serialization::encode_vi32(cbuf->get_data_ptr_address(), attempt);
    Serialization::encode_vstr(cbuf->get_data_ptr_address(), location.c_str());
    Serialization::encode_i32(cbuf->get_data_ptr_address(), ranges.size());
    for(size_t ii=0; ii<ranges.size(); ++ii)
      ranges[ii].encode(cbuf->get_data_ptr_address());
    Serialization::encode_i32(cbuf->get_data_ptr_address(), timeout_ms);

    return cbuf;
  }

} // namespace Hypertable
