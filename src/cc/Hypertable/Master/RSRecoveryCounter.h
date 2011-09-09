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

#ifndef HYPERTABLE_RSRECOVERYCOUNTER_H
#define HYPERTABLE_RSRECOVERYCOUNTER_H

#include "Common/ReferenceCount.h"
#include "Common/Time.h"

#include "Hypertable/Lib/Types.h"

namespace Hypertable {

  /**
   * Tracks outstanding RangeServer recover requests.
   */
  class RSRecoveryCounter : public ReferenceCount {
  public:

    struct Result {
      QualifiedRangeSpec range;
      int error;
    };

    RSRecoveryCounter(uint32_t attempt)
      : m_attempt(attempt), m_done(false), m_errors(false), m_timed_out(false) { }

    void add(const vector<QualifiedRangeSpec> &ranges) {
      ScopedLock lock(m_mutex);
      foreach (QualifiedRangeSpec &range, ranges)
        m_outstanding_ranges.push_back(range);
    }

    void result_callback(uint32_t attempt, vector<Result> &results) {
      ScopedLock lock(m_mutex);

      if (!m_outstanding_ranges.size() || attempt != m_attempt) {
        HT_WARN_OUT << "Received non-pending event" << HT_END;
        return;
      }

      foreach(Result &rr, results) {
        set<QualifiedRangeSpec>::iterator set_it = m_outstanding_ranges.find(rr.range);
        if (set_it != m_outstanding_ranges.end()) {
          rr.range = *set_it;
          m_results.push_back(rr);
          m_outstanding_ranges.erase(set_it);
        }
        else
          HT_WARN_OUT << "Received complete notification for non-pending range "
              << range << HT_END;
      }
      if (!m_outstanding_ranges.size())
        m_cond.notify_all();
    }

    bool wait_for_completion(Timer &timer) {
      ScopedLock lock(m_mutex);
      boost::xtime expire_time;

      timer.start();

      while (m_outstanding_ranges.size()) {
        boost::xtime_get(&expire_time, boost::TIME_UTC);
        xtime_add_millis(expire_time, timer.remaining());
        if (!m_cond.timed_wait(lock, expire_time)) {
          HT_INFO_OUT << "RSRecoveryCounter timed out" << HT_END;
          m_errors = true;
          m_timed_out = true;
          foreach (QualifiedRangeSpec &range, m_outstanding_ranges) {
            Result rr;
            rr.range = range;
            rr.error = Error::REQUEST_TIMEOUT;
            m_results.push_back(rr);
          }
          m_outstanding_ranges.clear();
        }
      }
      m_done = true;
      return m_errors;
    }

    vector<Result> &get_results() {
      ScopedLock lock(m_mutex);
      HT_ASSERT(m_done);
      return m_results;
    }

    bool timed_out() {
      ScopedLock lock(m_mutex);
      return m_timed_out;
    }

    uint32_t get_attempt() const { return m_attempt; }

    void report_range_errors(const vector<QualifiedRangeSpec> &ranges, int error) {
      ScopedLock lock(m_mutex);
      set<QualifiedRangeSpec>::iterator set_it;

      foreach(QualifiedRangeSpec &range, ranges) {
        set_it = m_outstanding_ranges.find(range);
        if (set_it != m_outstanding_ranges.end()) {
          Result rr;
          rr.range = *set_it;
          rr.error = error;
          m_results.push_back(rr);
          m_outstanding_ranges.erase(set_it);
        }
      }
    }

  protected:
    uint32_t m_attempt;
    Mutex m_mutex;
    boost::condition m_cond;
    size_t m_outstanding;
    bool m_errors;
    bool m_done;
    bool m_timed_out;
    vector<Result> m_results;
    set<QualifiedRangeSpec> m_outstanding_ranges;
  };
  typedef intrusive_ptr<RSRecoveryCounter> RSRecoveryCounterPtr;
}

#endif // HYPERTABLE_RSRECOVERYCOUNTER_H
