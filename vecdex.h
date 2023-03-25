/*
 * Copyright (C) 2023 Ronsor Labs. All rights reserved.
 * This software is free software provided to you under the terms of the MIT
 * license. For more information, see the included `LICENSE` file.
 *
 * VecDex: SQLite vector extensions.
 */

#ifndef VECDEX_H
#define VECDEX_H
#include "sqlite3.h"

#ifdef STATIC_VECDEX
int sqlite3_vecdex_init(sqlite3 *db, char **pzErrMsg);
#endif
#endif
