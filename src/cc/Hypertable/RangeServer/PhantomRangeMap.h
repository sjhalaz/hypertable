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

#ifndef HYPERTABLE_PHANTOMRANGEMAP_H
#define HYPERTABLE_PHANTOMRANGEMAP_H

#include <map>
#include <string>

#include "Common/ReferenceCount.h"
#include "Common/Mutex.h"

#include "Hypertable/Lib/Types.h"

#include "PhantomRange.h"

namespace Hypertable {
  using namespace std;
  /**
   * Provides a mapping from table name to TableInfo object.
   */
  class PhantomRangeMap : public ReferenceCount {
  public:
    PhantomRangeMap() { }
    virtual ~PhantomRangeMap() { }

    /**
     * Insert range in map
     *
     * @param range range spec
     * @param schema table schema
     * @param fragments commit log fragments
     */
    void insert(const QualifiedRangeStateSpec &range_spec, SchemaPtr &schema,
                const vector<uint32_t> &fragments);

    /**
     * Gets the phantom range if it is in map
     *
     * @param range range spec
     * @param phantom_range phantom range
     */
    void get(const QualifiedRangeStateSpec &range_spec, PhantomRangePtr &phantom_range);

    /**
     * Gets the phantom range, if not in map, insert first
     *
     * @param range range spec
     * @param schema table schema
     * @param fragments fragments to be played
     * @param phantom_range phantom range
     */
    void get(const QualifiedRangeStateSpec &range, SchemaPtr &schema,
             const vector<uint32_t> &fragments, PhantomRangePtr &phantom_range);
    /**
     * Remove range from map
     *
     * @param range range spec
     */
    void remove(const QualifiedRangeStateSpec &range_spec);

    /**
     * Remove range from map
     *
     * @param range range spec
     */
    void remove(const QualifiedRangeSpec &range_spec);

    /**
     * Get vector of all phantom ranges in map
     * @param range_vec will contain all ranges in map
     */
    void get_all(std::vector<PhantomRangePtr> &range_vec);

    /**
     * Get number of phantom ranges in map
     *
     * @return number of phantom ranges in map
     */
    size_t size();

  private:
    typedef std::map<QualifiedRangeStateSpec, PhantomRangePtr> Map;
    typedef std::pair<Map::iterator, bool> MapInsRec;

    Mutex         m_mutex;
    Map           m_map;
  };

  typedef boost::intrusive_ptr<PhantomRangeMap> PhantomRangeMapPtr;
}

#endif // HYPERTABLE_PHANTOMRANGEMAP_H
