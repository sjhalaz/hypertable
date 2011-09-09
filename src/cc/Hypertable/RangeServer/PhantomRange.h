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

#ifndef HYPERTABLE_PHANTOMRANGE_H
#define HYPERTABLE_PHANTOMRANGE_H

#include <map>
#include <vector>

#include "Common/String.h"

#include "Hypertable/Lib/Types.h"
#include "Hypertable/Lib/FileSystem.h"

#include "Range.h"
#include "FragmentData.h"

namespace Hypertable {
  using namespace std;
  /**
   * Represents a table row range.
   */
  class PhantomRange {

  public:
    enum State {
      INIT=0,
      FINISHED_REPLAY=1,
      RANGE_CREATED=2,
      RANGE_PREPARED=3
    };

    PhantomRange(const QualifiedRangeSpec &range, SchemaPtr &schema,
                 const vector<uint32_t> &fragments);
    ~PhantomRange() {}
    /**
     *
     * @param fragment fragment id
     * @param more if false this is the last data for this fragment
     * @param event contains data fort his fragment
     * @return true if the add succeded, false means the fragment is already complete
     */
    bool add(uint32_t fragment, bool more, EventPtr &event);
    void finish_fragment(uint32_t fragment);
    int get_state();
    void create_range(RangePtr &range);
    QualifiedRangeSpec get_qualified_range() { return m_spec; }

    void purge_incomplete_fragments();
    void get_incomplete_fragments(vector<uint32_t> &fragments);
    RangePtr create_range_and_transfer_log(MasterClientPtr &master_client,
        TableInfo &table_info, FileSystemPtr &log_dfs, String &log_dir);
    RangePtr get_range() {
      ScopedLock lock(m_mutex);
      return m_range;
    }
    void populate_range_and_log(FileSystemPtr &log_dfs);
    CommitLogPtr get_transfer_log();

  private:
    typedef std::map<uint32_t, FragmentDataPtr> FragmentMap;
    Mutex            m_mutex;
    FragmentMap      m_fragments;
    QualifiedRangeSpecManaged m_spec;
    SchemaPtr        m_schema;
    size_t           m_outstanding;
    RangePtr         m_range;
    CommitLogPtr     m_transfer_log;
    int              m_state;
  };

  typedef intrusive_ptr<PhantomRange> PhantomRangePtr;

} // namespace Hypertable

#endif // HYPERTABLE_PHANTOMRANGE_H
