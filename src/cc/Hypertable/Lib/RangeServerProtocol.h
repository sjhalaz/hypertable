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

#ifndef HYPERTABLE_RANGESERVERPROTOCOL_H
#define HYPERTABLE_RANGESERVERPROTOCOL_H

#include "AsyncComm/Protocol.h"

#include "Common/StaticBuffer.h"

#include "RangeState.h"
#include "ScanSpec.h"
#include "Types.h"
#include "RangeServerRecoveryPlan.h"

namespace Hypertable {

  /** Generates RangeServer protocol request messages */
  class RangeServerProtocol : public Protocol {

  public:
    static const uint64_t COMMAND_LOAD_RANGE               = 0;
    static const uint64_t COMMAND_UPDATE                   = 1;
    static const uint64_t COMMAND_CREATE_SCANNER           = 2;
    static const uint64_t COMMAND_FETCH_SCANBLOCK          = 3;
    static const uint64_t COMMAND_COMPACT                  = 4;
    static const uint64_t COMMAND_STATUS                   = 5;
    static const uint64_t COMMAND_SHUTDOWN                 = 6;
    static const uint64_t COMMAND_DUMP                     = 7;
    static const uint64_t COMMAND_DESTROY_SCANNER          = 8;
    static const uint64_t COMMAND_DROP_TABLE               = 9;
    static const uint64_t COMMAND_DROP_RANGE               = 10;
    static const uint64_t COMMAND_REPLAY_BEGIN             = 11;
    static const uint64_t COMMAND_REPLAY_LOAD_RANGE        = 12;
    static const uint64_t COMMAND_REPLAY_UPDATE            = 13;
    static const uint64_t COMMAND_REPLAY_COMMIT            = 14;
    static const uint64_t COMMAND_GET_STATISTICS           = 15;
    static const uint64_t COMMAND_UPDATE_SCHEMA            = 16;
    static const uint64_t COMMAND_COMMIT_LOG_SYNC          = 17;
    static const uint64_t COMMAND_CLOSE                    = 18;
    static const uint64_t COMMAND_WAIT_FOR_MAINTENANCE     = 19;
    static const uint64_t COMMAND_ACKNOWLEDGE_LOAD         = 20;
    static const uint64_t COMMAND_RELINQUISH_RANGE         = 21;
    static const uint64_t COMMAND_HEAPCHECK                = 22;
    static const uint64_t COMMAND_METADATA_SYNC            = 23;
    static const uint64_t COMMAND_PLAY_FRAGMENTS           = 24;
    static const uint64_t COMMAND_PHANTOM_LOAD             = 25;
    static const uint64_t COMMAND_PHANTOM_UPDATE           = 26;
    static const uint64_t COMMAND_PHANTOM_PREPARE_RANGES   = 27;
    static const uint64_t COMMAND_PHANTOM_COMMIT_RANGES    = 28;
    static const uint64_t COMMAND_MAX                      = 29;

    static const char *m_command_strings[];

    enum RangeGroup {
      GROUP_METADATA_ROOT,
      GROUP_METADATA,
      GROUP_SYSTEM,
      GROUP_USER
    };

    // The flags shd be the same as in Hypertable::TableMutator.
    enum {
      /* Don't force a commit log sync on update */
      UPDATE_FLAG_NO_LOG_SYNC        = 0x0001,
      UPDATE_FLAG_IGNORE_UNKNOWN_CFS = 0x0002
    };

    // Flags for
    enum {
      COMPACT_FLAG_ROOT     = 0x0001,
      COMPACT_FLAG_METADATA = 0x0002,
      COMPACT_FLAG_SYSTEM   = 0x0004,
      COMPACT_FLAG_USER     = 0x0008,
      COMPACT_FLAG_ALL      = 0x000F
    };

    static String compact_flags_to_string(uint32_t flags);

    /** Creates a "compact" request message
     *
     * @param table_id table identifier
     * @param flags compact flags
     * @return protocol message
     */
    static CommBuf *create_request_compact(const String &table_id, uint32_t flags);

    /** Creates a "metadata_sync" request message
     *
     * @param table_id table identifier
     * @param flags metadata_sync flags
     * @param columns names of columns to sync
     * @return protocol message
     */
    static CommBuf *create_request_metadata_sync(const String &table_id, uint32_t flags,
                                                 std::vector<String> &columns);

    /** Creates a "load range" request message
     *
     * @param table table identifier
     * @param range range specification
     * @param transfer_log transfer log to replay
     * @param range_state range state
     * @param needs_compaction if true the range needs to be compacted after load
     * @return protocol message
     */
    static CommBuf *create_request_load_range(const TableIdentifier &table,
        const RangeSpec &range, const char *transfer_log,
        const RangeState &range_state, bool needs_compaction);

    /** Creates an "update" request message.  The data argument holds a
     * sequence of key/value pairs.  Each key/value pair is encoded as two
     * variable lenght ByteStringrecords back-to-back.  This method transfers
     * ownership of the data buffer to the CommBuf that gets returned.
     *
     * @param table table identifier
     * @param count number of key/value pairs in buffer
     * @param buffer buffer holding key/value pairs
     * @param flags update flags
     * @return protocol message
     */
    static CommBuf *create_request_update(const TableIdentifier &table,
                                          uint32_t count, StaticBuffer &buffer, uint32_t flags);

    /** Creates an "update schema" message. Used to update schema for a
     * table
     *
     * @param table table identifier
     * @param schema the new schema
     * @return protocol message
     */
    static CommBuf *create_request_update_schema(
        const TableIdentifier &table, const String &schema);

    /** Creates an "commit_log_sync" message. Used to make previous range server updates
     * are syncd to the commit log
     * @param table table identifier
     * @return protocol message
     */
    static CommBuf *create_request_commit_log_sync(const TableIdentifier &table);

    /** Creates a "create scanner" request message.
     *
     * @param table table identifier
     * @param range range specification
     * @param scan_spec scan specification
     * @return protocol message
     */
    static CommBuf *create_request_create_scanner(const TableIdentifier &table,
        const RangeSpec &range, const ScanSpec &scan_spec);

    /** Creates a "destroy scanner" request message.
     *
     * @param scanner_id scanner ID returned from a "create scanner" request
     * @return protocol message
     */
    static CommBuf *create_request_destroy_scanner(int scanner_id);

    /** Creates a "fetch scanblock" request message.
     *
     * @param scanner_id scanner ID returned from a "create scanner" request
     * @return protocol message
     */
    static CommBuf *create_request_fetch_scanblock(int scanner_id);

    /** Creates a "status" request message.
     *
     * @return protocol message
     */
    static CommBuf *create_request_status();

    /** Creates a "close" request message.
     *
     * @return protocol message
     */
    static CommBuf *create_request_close();

    /** Creates a "wait_for_maintenance" request message.
     *
     * @return protocol message
     */
    static CommBuf *create_request_wait_for_maintenance();

    /** Creates a "shutdown" request message.
     *
     * @return protocol message
     */
    static CommBuf *create_request_shutdown();

    /** Creates a "dump" command (for testing)
     *
     * @param label controls what to dump
     * @param outfile name of file to dump to
     * @return protocol message
     */
    static CommBuf *create_request_dump(const String &outfile,
					bool nokeys);

    /** Creates a "drop table" request message.
     *
     * @param table table identifier
     * @return protocol message
     */
    static CommBuf *create_request_drop_table(const TableIdentifier &table);

    /** Creates a "replay begin" request message.
     *
     * @param group replay group to begin (METADATA_ROOT, METADATA, USER)
     * @return protocol message
     */
    static CommBuf *create_request_replay_begin(uint16_t group);

    /** Creates a "replay load range" request message.
     *
     * @return protocol message
     */
    static CommBuf *create_request_replay_load_range(const TableIdentifier &,
        const RangeSpec &range, const RangeState &range_state);

    /** Creates a "replay update" request message.  The data argument holds a
     * sequence of blocks.  Each block consists of ...
     *
     * @param buffer buffer holding updates to replay
     * @return protocol message
     */
    static CommBuf *create_request_replay_update(StaticBuffer &buffer);

    /** Creates a "replay commit" request message.
     *
     * @return protocol message
     */
    static CommBuf *create_request_replay_commit();

    /** Creates a "drop range" request message.
     *
     * @param table table identifier
     * @param range range specification
     * @return protocol message
     */
    static CommBuf *create_request_drop_range(const TableIdentifier &table,
                                              const RangeSpec &range);

    /** Creates a "acknowledge load" request message.
     *
     * @param ranges range specification vector
     * @return protocol message
     */
    static CommBuf *create_request_acknowledge_load(const vector<QualifiedRangeSpec*> &ranges);

    /** Creates a "get statistics" request message.
     *
     * @return protocol message
     */
    static CommBuf *create_request_get_statistics();

    /** Creates a "relinquish range" request message.
     *
     * @param table table identifier
     * @param range range specification
     * @return protocol message
     */
    static CommBuf *create_request_relinquish_range(const TableIdentifier &table,
                                                    const RangeSpec &range);

    /** Creates a "heapcheck" request message.
     *
     * @param outfile name of file to dump heap stats to
     * @return protocol message
     */
    static CommBuf *create_request_heapcheck(const String &outfile);

    /** Creates a "play_fragments" request message.
     *
     * @param op_id id of the calling recovery operation
     * @param attempt id of the replay attempt -- needed in case of master failure
     * @param location location of the server being recovered
     * @param type type of ranges being recovered
     * @param fragments fragments being requested for replay
     * @param load_plan recovery load plan
     * @param replay_timeout timeout for replay to finish
     */
    static CommBuf *create_request_play_fragments(int64_t op_id, uint32_t attempt,
        const String &location, int type, const std::vector<uint32_t> &fragments,
        const RangeServerRecoveryLoadPlan &load_plan, uint32_t replay_timeout);

    /** Creates a "play_fragments" request message.
     *
     * @param location location of server being recovered
     * @param fragments fragments being replayed
     * @param ranges range specs to be loaded
     */
    static CommBuf *create_request_phantom_load(const String &location,
        const vector<uint32_t> &fragments,
        const std::vector<QualifiedRangeSpec> &ranges);

    /** Creates a "phantom_update" request message.
     *
     * @param location server being recovered
     * @param range range being updated
     * @param fragment fragment updates belong to
     * @param more if false then this fragment is complete
     * @param buffer update buffer
     */
    static CommBuf *create_request_phantom_update(const QualifiedRangeSpec &range,
        const String &location, uint32_t fragment, bool more, StaticBuffer &buffer);

    /** Creates a "phantom_prepare_ranges" request message.
     *
     * @param op_id id of the calling recovery operation
     * @param attempt id of the replay attempt -- needed in case of master failure
     * @param location location of the server being recovered
     * @param ranges ranges to be prepared
     * @param timeout_ms timeout
     */
    static CommBuf *create_request_phantom_prepare_ranges(int64_t op_id, uint32_t attempt,
        const String &location, const std::vector<QualifiedRangeSpec> &ranges,
        uint32_t timeout_ms);

    /** Creates a "phantom_commit_ranges" request message.
     *
     * @param op_id id of the calling recovery operation
     * @param attempt id of the replay attempt -- needed in case of master failure
     * @param location location of the server being recovered
     * @param ranges ranges to be commit
     * @param timeout_ms timeout
     */
    static CommBuf *create_request_phantom_commit_ranges(int64_t op_id, uint32_t attempt,
        const String &location, const std::vector<QualifiedRangeSpec> &ranges,
        uint32_t timeout_ms);

    virtual const char *command_text(uint64_t command);
  };
}

#endif // HYPERTABLE_RANGESERVERPROTOCOL_H
