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
#include "PhantomRange.h"

PhantomRange::PhantomRange(const QualifiedRangeSpec &range, SchemaPtr &schema,
    const vector<uint32_t> &fragments) : m_spec(range),
    m_schema(schema), m_outstanding(fragments.size()), m_state(INIT) {
  foreach(uint32_t fragment, m_fragments)
    m_fragments[fragment] = new FragmentData;
}

int PhantomRange::get_state() {
  ScopedLock lock(m_mutex);
  return m_state;
}

bool PhantomRange::add(uint32_t fragment, bool more, EventPtr &event) {
  ScopedLock lock(m_mutex);
  FragmentMap::iterator it = m_fragments.find(fragment);
  HT_ASSERT(it != m_fragments.end());

  if (it->second->complete()) {
    // fragment is already complete
    return false;
  }
  else {
    HT_ASSERT(m_outstanding);
    if (!more) {
      --m_outstanding;
      if (!m_outstanding)
        m_state = FINISHED_REPLAY;
    }
    it->second->add(more, event);
  }
  return true;
}

void PhantomRange::get_incomplete_fragments(vector<uint32_t> &incomplete_fragments) {
  ScopedLock lock(m_mutex);
  FragmentMap::iterator it = m_fragments.begin();
  for(; it != m_fragments.end(); ++it)
    if (!it->second->complete())
      incomplete_fragments.push_back(it->first);
}

void PhantomRange::purge_incomplete_fragments() {
  ScopedLock lock(m_mutex);
  FragmentMap::iterator it = m_fragments.begin();
  for(; it != m_fragments.end(); ++it)
    if (!it->second->complete())
      it->second->clear();
}

void PhantomRange::create_range_and_transfer_log(MasterClientPtr &master_client,
  TableInfo &table_info, FileSystemPtr &log_dfs, String &log_dir) {

  char md5DigestStr[33];
  time_t now;
  ScopedLock lock(m_mutex);

  if (m_state >= RANGE_CREATED) {
    HT_WARN_OUT << "Range already created for phantom range " << m_spec << HT_END;
    return m_range;
  }

  RangeState range_state;
  range_state.state = RangeState::PHANTOM;

  m_range = new Range(master_client, &m_spec.table, m_schema, &m_spec.range,
                      table_info.get(), &range_state, true);
  MetaLog::EntityRange *metalog_entity = m_range->metalog_entity();
  // Create transfer log
  md5_trunc_modified_base64(m_spec.range.end_row, md5DigestStr);
  md5DigestStr[16] = 0;

  do {
    if (now != 0)
      poll(0, 0, 1200);
    now = time(0);
    metalog_entity->state.set_transfer_log(log_dir + "/" + m_spec.table.id + "/" +
        md5DigestStr + "-" + (int)now);
  }
  while (log_dfs->exists(metalog_entity->state.transfer_log));
  log_dfs->mkdirs(metalog_entity->state.transfer_log);

  m_state = RANGE_CREATED;
  return m_range;
}

void PhantomRange::populate_range_and_log(FileSystemPtr &log_dfs) {
  ScopedLock lock(m_mutex);
  if (m_state >= RANGE_LOADED) {
    HT_WARN_OUT << "Range already loaded for phantom range " << m_spec << HT_END;
    return ;
  }
  MetaLog::EntityRange *metalog_entity = m_range->metalog_entity();
  String log_file = metalog_entity->state.transfer_log;
  m_transfer_log = new CommitLog(log_dfs, log_dir, false);
  DynamicBuffer dbuf;
  int64_t latest_revision;

  foreach (FragmentMap::value_type &vv, m_fragments) {
    dbuf.clear();
    vv.second->merge(m_range, dbuf, &latest_revision);
    m_transfer_log->write(dbuf, latest_revision, false);
  }
  m_transfer_log->close();
  m_state = RANGE_PREPARED;
}

CommitLogPtr PhantomRange::get_transfer_log() {
  ScopedLock lock(m_mutex);
  return m_transfer_log;
}
