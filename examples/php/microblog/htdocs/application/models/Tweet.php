<?
/**
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


class Tweet
{
  public function getId() {
    return ($this->_id);
  }

  public function setId($id) {
    $this->_id=$id;
  }

  public function getAuthor() {
    $a=preg_split('/\.\d+$/', $this->_id);
    return ($a[0]);
  }

  public function getTimestamp() {
    return ($this->_timestamp);
  }

  public function setTimestamp($timestamp) {
    $this->_timestamp=$timestamp;
  }

  public function createId($author) {
    $a=gettimeofday();
    $this->_id=sprintf('%s.%u%u', $author, $a['sec'], $a['usec']/1000);
  }

  public function getMessage() {
    return ($this->_message);
  }

  public function setMessage($message) {
    $this->_message=$message;
  }

  protected $_id;
  protected $_timestamp;
  protected $_message;
}

?>


