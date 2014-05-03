;;; sqlingvo.el --- SQLingvo: A SQL DSL in Clojure  -*- lexical-binding: t; -*-

;; Copyright (C) 2014 Roman Scherer

;; Author: Roman Scherer <roman@burningswell.com>
;; Maintainer: Roman Scherer <roman@burningswell.com>
;; Version: 0.1.0
;; Keywords: indentation
;; URL: https://github.com/r0man/sqlingvo
;; Package-Requires: ((clojure-mode "2.1.1"))

;; This file is NOT part of GNU Emacs.

;; This program is free software; you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation; either version 3, or (at your option)
;; any later version.

;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with GNU Emacs; see the file COPYING.  If not, write to the
;; Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
;; Boston, MA 02110-1301, USA.

;;; Commentary:

;; Better indentation for SQLingvo forms in Clojure files.

;;; Code:

(add-hook
 'clojure-mode-hook
 (lambda ()
   (define-clojure-indent
     (copy 2)
     (create-table 1)
     (delete 1)
     (drop-table 1)
     (insert 2)
     (select 1)
     (truncate 1)
     (update 2))))

;;; sqlingvo.el ends here
