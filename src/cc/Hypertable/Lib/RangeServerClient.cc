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
#include "Common/Config.h"
#include "Common/Error.h"
#include "Common/StringExt.h"
#include "Common/Serialization.h"

#include "AsyncComm/DispatchHandlerSynchronizer.h"

#include "RangeServerClient.h"
#include "ScanBlock.h"

using namespace Hypertable;
using namespace Hypertable::Config;


RangeServerClient::RangeServerClient(Comm *comm, uint32_t timeout_ms)
  : m_comm(comm), m_default_timeout_ms(timeout_ms) {
  if (timeout_ms == 0)
    m_default_timeout_ms = get_i32("Hypertable.Request.Timeout");
}


RangeServerClient::~RangeServerClient() {
}


void
RangeServerClient::compact(const CommAddress &addr, const String &table_id, uint32_t flags) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(RangeServerProtocol::create_request_compact(table_id, flags));

  send_message(addr, cbp, &sync_handler, m_default_timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW((int)Protocol::response_code(event),
             String("RangeServer compact() failure : ")
             + Protocol::string_format_message(event));
}

void
RangeServerClient::compact(const CommAddress &addr, const String &table_id, uint32_t flags,
                           DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::create_request_compact(table_id, flags));
  send_message(addr, cbp, handler, m_default_timeout_ms);
}

void
RangeServerClient::compact(const CommAddress &addr, const String &table_id, uint32_t flags,
                           DispatchHandler *handler, Timer &timer) {
  CommBufPtr cbp(RangeServerProtocol::create_request_compact(table_id, flags));
  send_message(addr, cbp, handler, timer.remaining());
}


void
RangeServerClient::metadata_sync(const CommAddress &addr, const String &table_id, uint32_t flags,
                                 std::vector<String> &columns) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(RangeServerProtocol::create_request_metadata_sync(table_id, flags, columns));

  send_message(addr, cbp, &sync_handler, m_default_timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW((int)Protocol::response_code(event),
             String("RangeServer metadata_sync() failure : ")
             + Protocol::string_format_message(event));
}


void
RangeServerClient::load_range(const CommAddress &addr,
    const TableIdentifier &table, const RangeSpec &range,
    const char *transfer_log, const RangeState &range_state, bool needs_compaction) {
  do_load_range(addr, table, range, transfer_log, range_state, needs_compaction,
                m_default_timeout_ms);
}

void
RangeServerClient::load_range(const CommAddress &addr,
    const TableIdentifier &table, const RangeSpec &range,
    const char *transfer_log, const RangeState &range_state, bool needs_compaction,
    Timer &timer) {
  do_load_range(addr, table, range, transfer_log, range_state, needs_compaction,
                timer.remaining());
}

void
RangeServerClient::do_load_range(const CommAddress &addr,
    const TableIdentifier &table, const RangeSpec &range,
    const char *transfer_log, const RangeState &range_state, bool needs_compaction,
    uint32_t timeout_ms) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(RangeServerProtocol::create_request_load_range(table, range,
                 transfer_log, range_state, needs_compaction));

  send_message(addr, cbp, &sync_handler, timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW((int)Protocol::response_code(event),
               String("RangeServer load_range() failure : ")
               + Protocol::string_format_message(event));

}

void
RangeServerClient::acknowledge_load(const CommAddress &addr,
                                    const vector<QualifiedRangeSpec*> &ranges,
                                    map<QualifiedRangeSpec, int> &response_map) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(RangeServerProtocol::create_request_acknowledge_load(ranges));
  foreach (QualifiedRangeSpec *qrs, ranges) {
    response_map[*qrs] = Error::NO_RESPONSE;
  }

  send_message(addr, cbp, &sync_handler, m_default_timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW((int)Protocol::response_code(event),
             String("RangeServer acknowledge_load() failure : ")
             + Protocol::string_format_message(event));

  const uint8_t *decode_ptr = event->payload + 4;
  size_t decode_remain = event->payload_len - 4;
  int nn, error;
  QualifiedRangeSpec qrs;
  map<QualifiedRangeSpec, int>::iterator it;
  if ((error = (int)Protocol::response_code(event) != Error::OK))
    HT_THROW(error, "Received error in acknowledge_load() response");

  nn = Serialization::decode_i32(&decode_ptr, &decode_remain);
  HT_INFO_OUT << "nn=" << nn << HT_END;
  HT_ASSERT(nn == (int)response_map.size());

  for(int ii=0; ii<nn; ++ii) {
    qrs.decode(&decode_ptr, &decode_remain);
    it = response_map.find(qrs);
    if (it == response_map.end()) {
      HT_ERROR_OUT << "received ack for range " << qrs << " expected one of:" << HT_END;
      foreach (QualifiedRangeSpec *qrs, ranges) {
        HT_ERROR_OUT << *qrs << HT_END;
      }
      HT_ABORT;
    }

    error = Serialization::decode_vi32(&decode_ptr, &decode_remain);
    it->second = error;
  }
  return;
}

void
RangeServerClient::update(const CommAddress &addr, const TableIdentifier &table,
    uint32_t count, StaticBuffer &buffer, uint32_t flags, DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::create_request_update(table, count,
                                                            buffer, flags));
  send_message(addr, cbp, handler, m_default_timeout_ms);
}

void
RangeServerClient::update(const CommAddress &addr, const TableIdentifier &table,
                          uint32_t count, StaticBuffer &buffer, uint32_t flags,
                          DispatchHandler *handler, Timer &timer) {
  CommBufPtr cbp(RangeServerProtocol::create_request_update(table, count,
                                                            buffer, flags));
  send_message(addr, cbp, handler, timer.remaining());
}


void
RangeServerClient::update(const CommAddress &addr, const TableIdentifier &table,
                          uint32_t count, StaticBuffer &buffer, uint32_t flags) {
  do_update(addr, table, count, buffer, flags, m_default_timeout_ms);
}

void
RangeServerClient::update(const CommAddress &addr, const TableIdentifier &table,
                          uint32_t count, StaticBuffer &buffer, uint32_t flags,
                          Timer &timer) {
  do_update(addr, table, count, buffer, flags, timer.remaining());
}

void
RangeServerClient::do_update(const CommAddress &addr, const TableIdentifier &table,
                             uint32_t count, StaticBuffer &buffer, uint32_t flags,
                             uint32_t timeout_ms) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(RangeServerProtocol::create_request_update(table, count,
                                                            buffer, flags));
  send_message(addr, cbp, &sync_handler, timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW((int)Protocol::response_code(event),
             String("RangeServer update() failure : ")
             + Protocol::string_format_message(event));
}



void
RangeServerClient::create_scanner(const CommAddress &addr,
    const TableIdentifier &table, const RangeSpec &range,
    const ScanSpec &scan_spec, DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::create_request_create_scanner(table,
                 range, scan_spec));
  send_message(addr, cbp, handler, m_default_timeout_ms);
}

void
RangeServerClient::create_scanner(const CommAddress &addr,
    const TableIdentifier &table, const RangeSpec &range,
    const ScanSpec &scan_spec, DispatchHandler *handler,
    Timer &timer) {
  CommBufPtr cbp(RangeServerProtocol::create_request_create_scanner(table,
                 range, scan_spec));
  try {
    send_message(addr, cbp, handler, timer.remaining());
  }
  catch (Exception &e) {
    HT_THROW(e.code(), e.what());
  }
}


void
RangeServerClient::create_scanner(const CommAddress &addr,
    const TableIdentifier &table, const RangeSpec &range,
    const ScanSpec &scan_spec, ScanBlock &scan_block) {
  do_create_scanner(addr, table, range, scan_spec,
                    scan_block, m_default_timeout_ms);
}

void
RangeServerClient::create_scanner(const CommAddress &addr,
    const TableIdentifier &table, const RangeSpec &range,
    const ScanSpec &scan_spec, ScanBlock &scan_block,
    Timer &timer) {
  do_create_scanner(addr, table, range, scan_spec,
                    scan_block, timer.remaining());
}

void
RangeServerClient::do_create_scanner(const CommAddress &addr,
    const TableIdentifier &table, const RangeSpec &range,
    const ScanSpec &scan_spec, ScanBlock &scan_block,
    uint32_t timeout_ms) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(RangeServerProtocol::create_request_create_scanner(table,
                 range, scan_spec));
  send_message(addr, cbp, &sync_handler, timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW((int)Protocol::response_code(event),
             String("RangeServer create_scanner() failure : ")
             + Protocol::string_format_message(event));
  else {
    HT_ASSERT(scan_block.load(event) == Error::OK);
  }
}


void
RangeServerClient::destroy_scanner(const CommAddress &addr, int scanner_id,
                                   DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::
                 create_request_destroy_scanner(scanner_id));
  send_message(addr, cbp, handler, m_default_timeout_ms);
}

void
RangeServerClient::destroy_scanner(const CommAddress &addr, int scanner_id,
                                   DispatchHandler *handler, Timer &timer) {
  CommBufPtr cbp(RangeServerProtocol::
                 create_request_destroy_scanner(scanner_id));
  send_message(addr, cbp, handler, timer.remaining());
}


void
RangeServerClient::destroy_scanner(const CommAddress &addr, int scanner_id) {
  do_destroy_scanner(addr, scanner_id, m_default_timeout_ms);
}

void
RangeServerClient::destroy_scanner(const CommAddress &addr, int scanner_id,
                                   Timer &timer) {
  do_destroy_scanner(addr, scanner_id, timer.remaining());
}

void
RangeServerClient::do_destroy_scanner(const CommAddress &addr, int scanner_id,
                                      uint32_t timeout_ms) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(RangeServerProtocol::
                 create_request_destroy_scanner(scanner_id));
  send_message(addr, cbp, &sync_handler, timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW((int)Protocol::response_code(event),
             String("RangeServer destroy_scanner() failure : ")
             + Protocol::string_format_message(event));
}


void
RangeServerClient::fetch_scanblock(const CommAddress &addr, int scanner_id,
                                   DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::
                 create_request_fetch_scanblock(scanner_id));
  send_message(addr, cbp, handler, m_default_timeout_ms);
}

void
RangeServerClient::fetch_scanblock(const CommAddress &addr, int scanner_id,
                                   DispatchHandler *handler, Timer &timer) {
  CommBufPtr cbp(RangeServerProtocol::
                 create_request_fetch_scanblock(scanner_id));
  send_message(addr, cbp, handler, timer.remaining());
}


void
RangeServerClient::fetch_scanblock(const CommAddress &addr, int scanner_id,
                                   ScanBlock &scan_block) {
  do_fetch_scanblock(addr, scanner_id, scan_block, m_default_timeout_ms);
}

void
RangeServerClient::fetch_scanblock(const CommAddress &addr, int scanner_id,
                                   ScanBlock &scan_block, Timer &timer) {
  do_fetch_scanblock(addr, scanner_id, scan_block, timer.remaining());
}

void
RangeServerClient::do_fetch_scanblock(const CommAddress &addr, int scanner_id,
                                      ScanBlock &scan_block, uint32_t timeout_ms) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(RangeServerProtocol::
                 create_request_fetch_scanblock(scanner_id));
  send_message(addr, cbp, &sync_handler, timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW((int)Protocol::response_code(event),
             String("RangeServer fetch_scanblock() failure : ")
             + Protocol::string_format_message(event));
  else {
    HT_EXPECT(scan_block.load(event) == Error::OK,
              Error::FAILED_EXPECTATION);
  }
}


void
RangeServerClient::drop_table(const CommAddress &addr,
    const TableIdentifier &table, DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::create_request_drop_table(table));
  send_message(addr, cbp, handler, m_default_timeout_ms);
}

void
RangeServerClient::drop_table(const CommAddress &addr,
       const TableIdentifier &table, DispatchHandler *handler,
       Timer &timer) {
  CommBufPtr cbp(RangeServerProtocol::create_request_drop_table(table));
  send_message(addr, cbp, handler, timer.remaining());
}


void
RangeServerClient::drop_table(const CommAddress &addr,
                              const TableIdentifier &table) {
  do_drop_table(addr, table, m_default_timeout_ms);
}

void
RangeServerClient::do_drop_table(const CommAddress &addr,
                                 const TableIdentifier &table,
                                 uint32_t timeout_ms) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(RangeServerProtocol::create_request_drop_table(table));
  send_message(addr, cbp, &sync_handler, timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW((int)Protocol::response_code(event),
             String("RangeServer drop_table() failure : ")
             + Protocol::string_format_message(event));
}

void
RangeServerClient::update_schema(const CommAddress &addr,
    const TableIdentifier &table, const String &schema,
    DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::create_request_update_schema(table,
      schema));
  send_message(addr, cbp, handler, m_default_timeout_ms);
}

void
RangeServerClient::update_schema(const CommAddress &addr,
    const TableIdentifier &table, const String &schema,
    DispatchHandler *handler, Timer &timer) {
  CommBufPtr cbp(RangeServerProtocol::create_request_update_schema(table,
      schema));
  send_message(addr, cbp, handler, timer.remaining());
}

void
RangeServerClient::commit_log_sync(const CommAddress &addr, const TableIdentifier &table_id,
                                   DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::create_request_commit_log_sync(table_id));
  send_message(addr, cbp, handler, m_default_timeout_ms);
}

void
RangeServerClient::commit_log_sync(const CommAddress &addr, const TableIdentifier &table_id,
                                   DispatchHandler *handler, Timer &timer) {
  CommBufPtr cbp(RangeServerProtocol::create_request_commit_log_sync(table_id));
  send_message(addr, cbp, handler, timer.remaining());
}

void RangeServerClient::status(const CommAddress &addr) {
  do_status(addr, m_default_timeout_ms);
}

void RangeServerClient::status(const CommAddress &addr, Timer &timer) {
  do_status(addr, timer.remaining());
}

void RangeServerClient::do_status(const CommAddress &addr, uint32_t timeout_ms) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(RangeServerProtocol::create_request_status());
  send_message(addr, cbp, &sync_handler, timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW((int)Protocol::response_code(event),
             String("RangeServer status() failure : ")
             + Protocol::string_format_message(event));
}

void RangeServerClient::close(const CommAddress &addr) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(RangeServerProtocol::create_request_close());
  send_message(addr, cbp, &sync_handler, m_default_timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW((int)Protocol::response_code(event),
             String("RangeServer close() failure : ")
             + Protocol::string_format_message(event));
}

void RangeServerClient::wait_for_maintenance(const CommAddress &addr) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(RangeServerProtocol::create_request_wait_for_maintenance());
  send_message(addr, cbp, &sync_handler, m_default_timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW((int)Protocol::response_code(event),
             String("RangeServer wait_for_maintenance() failure : ")
             + Protocol::string_format_message(event));
}


void RangeServerClient::shutdown(const CommAddress &addr) {
  CommBufPtr cbp(RangeServerProtocol::create_request_shutdown());
  send_message(addr, cbp, 0, m_default_timeout_ms);
}

void RangeServerClient::dump(const CommAddress &addr,
			     String &outfile, bool nokeys) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(RangeServerProtocol::create_request_dump(outfile, nokeys));
  send_message(addr, cbp, &sync_handler, m_default_timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW((int)Protocol::response_code(event),
             String("RangeServer dump() failure : ")
             + Protocol::string_format_message(event));

}

void
RangeServerClient::get_statistics(const CommAddress &addr, StatsRangeServer &stats) {
  do_get_statistics(addr, stats, m_default_timeout_ms);
}

void
RangeServerClient::get_statistics(const CommAddress &addr, StatsRangeServer &stats, Timer &timer) {
  do_get_statistics(addr, stats, timer.remaining());
}

void
RangeServerClient::do_get_statistics(const CommAddress &addr, StatsRangeServer &stats,
                                     uint32_t timeout_ms) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(RangeServerProtocol::create_request_get_statistics());
  send_message(addr, cbp, &sync_handler, timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW((int)Protocol::response_code(event),
             String("RangeServer get_statistics() failure : ")
             + Protocol::string_format_message(event));

  size_t remaining = event->payload_len - 4;
  const uint8_t *ptr = event->payload + 4;

  stats.decode(&ptr, &remaining);
}

void
RangeServerClient::get_statistics(const CommAddress &addr, DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::create_request_get_statistics());
  send_message(addr, cbp, handler, m_default_timeout_ms);
}

void
RangeServerClient::get_statistics(const CommAddress &addr, DispatchHandler *handler,
                                  Timer &timer) {
  CommBufPtr cbp(RangeServerProtocol::create_request_get_statistics());
  send_message(addr, cbp, handler, timer.remaining());
}

void
RangeServerClient::decode_response_get_statistics(EventPtr &event, StatsRangeServer &stats) {
  int32_t error = Protocol::response_code(event);

  if (error != 0)
    HT_THROW((int)error, String("RangeServer get_statistics() failure : ")
             + Protocol::string_format_message(event));

  size_t remaining = event->payload_len - 4;
  const uint8_t *ptr = event->payload + 4;

  stats.decode(&ptr, &remaining);
}


void
RangeServerClient::replay_begin(const CommAddress &addr, uint16_t group,
                                DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::create_request_replay_begin(group));
  send_message(addr, cbp, handler, m_default_timeout_ms);
}

void
RangeServerClient::replay_begin(const CommAddress &addr, uint16_t group,
                                DispatchHandler *handler, Timer &timer) {
  CommBufPtr cbp(RangeServerProtocol::create_request_replay_begin(group));
  send_message(addr, cbp, handler, timer.remaining());
}

void
RangeServerClient::replay_load_range(const CommAddress &addr,
    const TableIdentifier &table, const RangeSpec &range,
    const RangeState &range_state, DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::create_request_replay_load_range(table,
                 range, range_state));
  send_message(addr, cbp, handler, m_default_timeout_ms);
}

void
RangeServerClient::replay_load_range(const CommAddress &addr,
    const TableIdentifier &table, const RangeSpec &range,
    const RangeState &range_state, DispatchHandler *handler,
    Timer &timer) {
  CommBufPtr cbp(RangeServerProtocol::create_request_replay_load_range(table,
                 range, range_state));
  send_message(addr, cbp, handler, timer.remaining());
}

void
RangeServerClient::replay_update(const CommAddress &addr, StaticBuffer &buffer,
                                 DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::create_request_replay_update(buffer));
  send_message(addr, cbp, handler, m_default_timeout_ms);
}

void
RangeServerClient::replay_update(const CommAddress &addr, StaticBuffer &buffer,
                                 DispatchHandler *handler, Timer &timer) {
  CommBufPtr cbp(RangeServerProtocol::create_request_replay_update(buffer));
  send_message(addr, cbp, handler, timer.remaining());
}


void
RangeServerClient::replay_commit(const CommAddress &addr,
                                 DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::create_request_replay_commit());
  send_message(addr, cbp, handler, m_default_timeout_ms);
}

void
RangeServerClient::replay_commit(const CommAddress &addr,
                                 DispatchHandler *handler, Timer &timer) {
  CommBufPtr cbp(RangeServerProtocol::create_request_replay_commit());
  send_message(addr, cbp, handler, timer.remaining());
}


void
RangeServerClient::drop_range(const CommAddress &addr,
    const TableIdentifier &table, const RangeSpec &range,
    DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::create_request_drop_range(table, range));
  send_message(addr, cbp, handler, m_default_timeout_ms);
}

void
RangeServerClient::drop_range(const CommAddress &addr,
    const TableIdentifier &table, const RangeSpec &range,
    DispatchHandler *handler, Timer &timer) {
  CommBufPtr cbp(RangeServerProtocol::create_request_drop_range(table, range));
  send_message(addr, cbp, handler, timer.remaining());
}

void RangeServerClient::relinquish_range(const CommAddress &addr,
                   const TableIdentifier &table, const RangeSpec &range) {
  do_relinquish_range(addr, table, range, m_default_timeout_ms);
}

void RangeServerClient::relinquish_range(const CommAddress &addr,
                                         const TableIdentifier &table,
                                         const RangeSpec &range, Timer &timer) {
  do_relinquish_range(addr, table, range, timer.remaining());
}

void
RangeServerClient::do_relinquish_range(const CommAddress &addr, const TableIdentifier &table,
                                       const RangeSpec &range, uint32_t timeout_ms) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(RangeServerProtocol::create_request_relinquish_range(table, range));
  send_message(addr, cbp, &sync_handler, timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW((int)Protocol::response_code(event),
             String("RangeServer relinquish_range() failure : ")
             + Protocol::string_format_message(event));
}

void RangeServerClient::heapcheck(const CommAddress &addr, String &outfile) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(RangeServerProtocol::create_request_heapcheck(outfile));
  send_message(addr, cbp, &sync_handler, m_default_timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW((int)Protocol::response_code(event),
             String("RangeServer heapcheck() failure : ")
             + Protocol::string_format_message(event));
}

void RangeServerClient::replay_fragments(const CommAddress &addr, int64_t op_id,
    uint32_t attempt, const String &recover_location, int type,
    const vector<uint32_t> &fragments, const RangeServerRecoveryReceiverPlan &plan,
    uint32_t timeout_ms) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(RangeServerProtocol::create_request_replay_fragments(op_id, attempt,
      recover_location, type, fragments, plan, timeout_ms));
  send_message(addr, cbp, &sync_handler, timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW((int)Protocol::response_code(event),
             String("RangeServer replay_fragments() failure : ")
             + Protocol::string_format_message(event));

}

void RangeServerClient::phantom_receive(const CommAddress &addr, const String &location,
    const vector<uint32_t> &fragments, const vector<QualifiedRangeStateSpec> &ranges) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(RangeServerProtocol::create_request_phantom_receive(location, fragments, ranges));
  send_message(addr, cbp, &sync_handler, m_default_timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW((int)Protocol::response_code(event),
             String("RangeServer phantom_receive() failure : ")
             + Protocol::string_format_message(event));
}

void RangeServerClient::phantom_update(const CommAddress &addr, const String &location,
    const QualifiedRangeSpec &range, uint32_t fragment, bool more, StaticBuffer &buffer,
    DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::create_request_phantom_update(range, location, fragment,
                 more, buffer));
  send_message(addr, cbp, handler, m_default_timeout_ms);
}

void RangeServerClient::phantom_prepare_ranges(const CommAddress &addr, int64_t op_id,
    uint32_t attempt, const String &location, const vector<QualifiedRangeSpec> &ranges,
    uint32_t timeout_ms) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(RangeServerProtocol::create_request_phantom_prepare_ranges(op_id, attempt,
      location, ranges, timeout_ms));
  send_message(addr, cbp, &sync_handler, timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW((int)Protocol::response_code(event),
             String("RangeServer phantom_prepare_ranges() failure : ")
             + Protocol::string_format_message(event));

}

void RangeServerClient::phantom_commit_ranges(const CommAddress &addr, int64_t op_id,
    uint32_t attempt, const String &location, const vector<QualifiedRangeSpec> &ranges,
    uint32_t timeout_ms) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(RangeServerProtocol::create_request_phantom_commit_ranges(op_id, attempt,
      location, ranges, timeout_ms));
  send_message(addr, cbp, &sync_handler, timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW((int)Protocol::response_code(event),
             String("RangeServer phantom_commit_ranges() failure : ")
             + Protocol::string_format_message(event));
}

void
RangeServerClient::send_message(const CommAddress &addr, CommBufPtr &cbp,
                                DispatchHandler *handler, uint32_t timeout_ms) {
  int error;

  if ((error = m_comm->send_request(addr, timeout_ms, cbp, handler))
      != Error::OK) {
    HT_WARNF("Comm::send_request to %s failed - %s",
             addr.to_str().c_str(), Error::get_text(error));
    HT_THROWF(error, "Comm::send_request to %s failed",
              addr.to_str().c_str());
  }
}
