/* 
 * Copyright (C) 2023 Ronsor Labs. All rights reserved.
 * This software is free software provided to you under the terms of the MIT
 * license. For more information, see the included `LICENSE` file.
 *
 * VecDex: SQLite vector extensions.
 */

#include <math.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "vecdex.h"

#ifndef STATIC_VECDEX
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1
#endif


#define SQLITE_PURE (SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC)
#define SQLITE_PURE_UTF8 (SQLITE_PURE | SQLITE_UTF8)

#define VEC_ALLOC_INCR 64
#define VEC_TO_BUF_SIZE(n) ((n) * sizeof(float))

static const float* sqlite3_value_vector(sqlite3_value *value, int* dim) {
  if (sqlite3_value_type(value) != SQLITE_BLOB) return NULL;

  int size = sqlite3_value_bytes(value);
  if ((size % sizeof(float)) != 0) {
    return NULL;
  }

  *dim = size / sizeof(float);
  return sqlite3_value_blob(value);
}

static int sqlite3_value_dim(sqlite3_value *value) {
  if (sqlite3_value_type(value) != SQLITE_BLOB) return 0;

  int dim;
  if (sqlite3_value_vector(value, &dim) == NULL) {
    return 0;
  }
  return dim;
}

#ifndef NDEBUG
/*
 * Print vector data to stdout.
 */
static void vectorDebugPrint(const float* vec, int dim, int showFull) {
  printf("[");
  for (int i = 0; i < dim; i++) {
    if (!showFull && dim > 128 && i > 16 && i < (dim - 16)) {
      printf(", ...");
      i = dim - 16 - 1;
      continue;
    }
    printf("%s%g", i != 0 ? ", " : "", vec[i]);
  }
  printf("]\n");
}

/*
 * Wrapper for vectorDebugPrint.
 */
static void vectorDebugFunc(sqlite3_context *ctx,
                            int argc, sqlite3_value **argv) {
  if (argc < 1) return;

  const float *vec;
  int dim;
  if ((vec = sqlite3_value_vector(argv[0], &dim)) == NULL)
    return;
  
  vectorDebugPrint(vec, dim, 0);
}
#endif

/*
 * Loosely "parse" JSON array into a vector.
 */
static float* vectorParseJson(const char* zJson, int jsonLen,
                              int* pVecDim, int getDimOnly) {
  float* ret = NULL;
  int len = 0, i = 0;

  const char* top = zJson + (jsonLen != -1 ? jsonLen : 0xFFFFF);
  while (zJson && *zJson && zJson < top) {
    if (strchr(" \t\v\n\r[,]", *zJson)) {
      zJson++;
      continue;
    }

    if (strchr("NI-+0123456789.", *zJson)) {
      if (!getDimOnly && len <= i) {
        len += VEC_ALLOC_INCR;
        ret = sqlite3_realloc(ret, VEC_TO_BUF_SIZE(len));
        if (ret == NULL) {
          goto failed;
        }
      }

      char *next = NULL;
      float value = strtof(zJson, &next);
      if (next == zJson) {
        goto failed;
      }
      if (!getDimOnly) {
        ret[i] = value;
      }

      i++;
      zJson = next;
      continue;
    }

failed:
    i = -1;
    sqlite3_free(ret);
    ret = NULL;
    break;
  }

  if (pVecDim) {
    *pVecDim = i;
  }
  return ret;
}

/*
 * Convert value to a vector, or return unchanged if it's already a vector.
 */
static void vectorFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  if (argc < 1) return;

  if (argc > 1 || sqlite3_value_type(argv[0]) == SQLITE_FLOAT
               || sqlite3_value_type(argv[0]) == SQLITE_INTEGER) {
    float* vec = sqlite3_malloc(VEC_TO_BUF_SIZE(argc));
    if (!vec) {
      sqlite3_result_error_code(ctx, SQLITE_NOMEM);
      return;
    }

    for (int i = 0; i < argc; i++) {
      int type = sqlite3_value_numeric_type(argv[i]);
      if (type == SQLITE_INTEGER) {
        vec[i] = (float)sqlite3_value_int(argv[i]);
      } else if (type == SQLITE_FLOAT) {
        vec[i] = (float)sqlite3_value_double(argv[i]);
      } else {
        free(vec);
        sqlite3_result_null(ctx);
        return;
      }
    }

    sqlite3_result_blob(ctx, vec, VEC_TO_BUF_SIZE(argc), sqlite3_free);
    return;
  }

  switch (sqlite3_value_type(argv[0])) {
    case SQLITE_BLOB: {
      int size = sqlite3_value_bytes(argv[0]);
      if ((size % sizeof(float)) != 0) {
        sqlite3_result_null(ctx);
        return;
      }

      sqlite3_result_blob(ctx, sqlite3_value_blob(argv[0]), size,
                          SQLITE_TRANSIENT);
      return;
    }
    case SQLITE_TEXT: {
      int dim = 0;
      float* data = vectorParseJson(sqlite3_value_text(argv[0]),
                                    sqlite3_value_bytes(argv[0]),
                                    &dim, 0);
      if (!data) {
        sqlite3_result_error_code(ctx, SQLITE_NOMEM);
        return;
      }

      sqlite3_result_blob(ctx, data, dim * sizeof(float), sqlite3_free);
      return;
    }
  }

  sqlite3_result_null(ctx);
  return;
}

/*
 * Return a vector of the specified dimension containing all zeroes.
 */
static void vector0Func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  if (argc < 1) return;

  sqlite3_result_zeroblob(ctx, sqlite3_value_int(argv[0]) * sizeof(float));
  return;
}

/*
 * Return the vector as a JSON string.
 */
static void vectorToJsonFunc(sqlite3_context *ctx,
                             int argc, sqlite3_value **argv) {
  if (argc < 1) return;

  const float *vec;
  int dim;
  if ((vec = sqlite3_value_vector(argv[0], &dim)) == NULL) {
    sqlite3_result_null(ctx);
    return;
  }

  sqlite3_str* str = sqlite3_str_new(sqlite3_context_db_handle(ctx)); 
  if (str == NULL) {
    sqlite3_result_null(ctx);
    return;
  }

  sqlite3_str_appendchar(str, 1, '[');
  for (int i = 0; i < dim; i++) {
    if (sqlite3_str_errcode(str) != SQLITE_OK) {
      sqlite3_result_error_code(ctx, sqlite3_str_errcode(str));
      return;
    }
    sqlite3_str_appendf(str, "%s%g", i != 0 ? "," : "", vec[i]);
  }
  sqlite3_str_appendchar(str, 1, ']');
  if (sqlite3_str_errcode(str) != SQLITE_OK) {
    sqlite3_result_error_code(ctx, sqlite3_str_errcode(str));
    return;
  }
  
  sqlite3_result_text(ctx, sqlite3_str_value(str), sqlite3_str_length(str),
                      SQLITE_TRANSIENT);
  sqlite3_str_finish(str);
  return;
}

/*
 * Compare two vectors.
 */
static void vectorCompareFunc(sqlite3_context *ctx,
                              int argc, sqlite3_value **argv) {
  if (argc < 2) return;

  const float *vecA, *vecB;
  int dimA, dimB;
  if ((vecA = sqlite3_value_vector(argv[0], &dimA)) == NULL) {
    sqlite3_result_null(ctx);
    return;
  } else if ((vecB = sqlite3_value_vector(argv[1], &dimB)) == NULL) {
    sqlite3_result_null(ctx);
    return;
  } else if (dimA != dimB) {
    sqlite3_result_null(ctx);
    return;
  }

  for (int i = 0; i < dimA; i++) {
    if (vecA[i] < vecB[i]) {
      sqlite3_result_int(ctx, -1);
      return;
    } else if (vecA[i] > vecB[i]) {
      sqlite3_result_int(ctx, 1);
      return;
    }
  }

  sqlite3_result_int(ctx, 0);
  return;
}

/*
 * Calculate cosine similarity of two vectors.
 */
static void vectorCosimFunc(sqlite3_context *ctx,
                            int argc, sqlite3_value **argv) {
  if (argc < 2) return;

  const float *vecA, *vecB;
  int dimA, dimB;
  if ((vecA = sqlite3_value_vector(argv[0], &dimA)) == NULL) {
    sqlite3_result_null(ctx);
    return;
  } else if ((vecB = sqlite3_value_vector(argv[1], &dimB)) == NULL) {
    sqlite3_result_null(ctx);
    return;
  } else if (dimA != dimB) {
    sqlite3_result_null(ctx);
    return;
  }

  double dotprod = 0.0, normA = 0.0, normB = 0.0;
  for (int i = 0; i < dimA; i++) {
    dotprod += vecA[i] * vecB[i];
    normA += vecA[i] * vecA[i];
    normB += vecB[i] * vecB[i];
  }

  sqlite3_result_double(ctx, dotprod / sqrt(normA * normB));
  return;
}

/*
 * Calculate L2 distance of two vectors.
 */
static void vectorDistFunc(sqlite3_context *ctx,
                           int argc, sqlite3_value **argv) {
  if (argc < 2) return;

  const float *vecA, *vecB;
  int dimA, dimB;
  if ((vecA = sqlite3_value_vector(argv[0], &dimA)) == NULL) {
    sqlite3_result_null(ctx);
    return;
  } else if ((vecB = sqlite3_value_vector(argv[1], &dimB)) == NULL) {
    sqlite3_result_null(ctx);
    return;
  } else if (dimA != dimB) {
    sqlite3_result_null(ctx);
    return;
  }

  double distance = 0.0, diff = 0.0;
  for (int i = 0; i < dimA; i++) {
    diff = vecA[i] - vecB[i];
    distance += diff * diff;
  }

  sqlite3_result_double(ctx, sqrt(distance));
  return;
}

/*
 * Get dimensions of a vector.
 */
static void vectorDimFunc(sqlite3_context *ctx,
                          int argc, sqlite3_value **argv) {
  if (argc < 1) return;

  int dim;
  if (sqlite3_value_vector(argv[0], &dim) == NULL) {
    sqlite3_result_null(ctx);
    return;
  }

  sqlite3_result_int(ctx, dim);
  return;
}

/*
 * Get average of a vector.
 */
static void vectorAvgFunc(sqlite3_context *ctx,
                          int argc, sqlite3_value **argv) {
  if (argc < 1) return;

  const float* vec;
  int dim;
  if ((vec = sqlite3_value_vector(argv[0], &dim)) == NULL) {
    sqlite3_result_null(ctx);
    return;
  }

  double final = 0.0;
  for (int i = 0; i < dim; i++) {
    final += vec[i];
  }
  final /= dim;

  sqlite3_result_double(ctx, final);
  return;
}

/*
 * Get L2 norm of a vector.
 */
static void vectorNormFunc(sqlite3_context *ctx,
                           int argc, sqlite3_value **argv) {
  if (argc < 1) return;

  const float* vec;
  int dim;
  if ((vec = sqlite3_value_vector(argv[0], &dim)) == NULL) {
    sqlite3_result_null(ctx);
    return;
  }

  double final = 0.0;
  for (int i = 0; i < dim; i++) {
    final += vec[i] * vec[i];
  }

  sqlite3_result_double(ctx, sqrt(final));
  return;
}

/*
 * Add two vectors.
 */
static void vectorAddFunc(sqlite3_context *ctx,
                          int argc, sqlite3_value **argv) {
  if (argc < 2) return;

  const float *vecA, *vecB;
  int dimA, dimB;
  if ((vecA = sqlite3_value_vector(argv[0], &dimA)) == NULL) {
    sqlite3_result_null(ctx);
    return;
  } else if ((vecB = sqlite3_value_vector(argv[1], &dimB)) == NULL) {
    sqlite3_result_null(ctx);
    return;
  } else if (dimA != dimB) {
    sqlite3_result_null(ctx);
    return;
  }

  float* vecC = sqlite3_malloc(VEC_TO_BUF_SIZE(dimA));
  if (!vecC) {
    sqlite3_result_error_code(ctx, SQLITE_NOMEM);
    return;
  }

  for (int i = 0; i < dimA; i++) {
    vecC[i] = vecA[i] + vecB[i];
  }

  sqlite3_result_blob(ctx, vecC, VEC_TO_BUF_SIZE(dimA), sqlite3_free);
  return;
}

/*
 * Subtract two vectors.
 */
static void vectorSubFunc(sqlite3_context *ctx,
                          int argc, sqlite3_value **argv) {
  if (argc < 2) return;

  const float *vecA, *vecB;
  int dimA, dimB;
  if ((vecA = sqlite3_value_vector(argv[0], &dimA)) == NULL) {
    sqlite3_result_null(ctx);
    return;
  } else if ((vecB = sqlite3_value_vector(argv[1], &dimB)) == NULL) {
    sqlite3_result_null(ctx);
    return;
  } else if (dimA != dimB) {
    sqlite3_result_null(ctx);
    return;
  }

  float* vecC = sqlite3_malloc(VEC_TO_BUF_SIZE(dimA));
  if (!vecC) {
    sqlite3_result_error_code(ctx, SQLITE_NOMEM);
    return;
  }

  for (int i = 0; i < dimA; i++) {
    vecC[i] = vecA[i] - vecB[i];
  }

  sqlite3_result_blob(ctx, vecC, VEC_TO_BUF_SIZE(dimA), sqlite3_free);
  return;
}

/*
 * Multiply two vectors.
 */
static void vectorMulFunc(sqlite3_context *ctx,
                          int argc, sqlite3_value **argv) {
  if (argc < 2) return;

  const float *vecA, *vecB;
  int dimA, dimB;
  if ((vecA = sqlite3_value_vector(argv[0], &dimA)) == NULL) {
    sqlite3_result_null(ctx);
    return;
  } else if ((vecB = sqlite3_value_vector(argv[1], &dimB)) == NULL) {
    sqlite3_result_null(ctx);
    return;
  } else if (dimA != dimB) {
    sqlite3_result_null(ctx);
    return;
  }

  float* vecC = sqlite3_malloc(VEC_TO_BUF_SIZE(dimA));
  if (!vecC) {
    sqlite3_result_error_code(ctx, SQLITE_NOMEM);
    return;
  }

  for (int i = 0; i < dimA; i++) {
    vecC[i] = vecA[i] * vecB[i];
  }

  sqlite3_result_blob(ctx, vecC, VEC_TO_BUF_SIZE(dimA), sqlite3_free);
  return;
}

/*
 * Divide two vectors.
 */
static void vectorDivFunc(sqlite3_context *ctx,
                          int argc, sqlite3_value **argv) {
  if (argc < 2) return;

  const float *vecA, *vecB;
  int dimA, dimB;
  if ((vecA = sqlite3_value_vector(argv[0], &dimA)) == NULL) {
    sqlite3_result_null(ctx);
    return;
  } else if ((vecB = sqlite3_value_vector(argv[1], &dimB)) == NULL) {
    sqlite3_result_null(ctx);
    return;
  } else if (dimA != dimB) {
    sqlite3_result_null(ctx);
    return;
  }

  float* vecC = sqlite3_malloc(VEC_TO_BUF_SIZE(dimA));
  if (!vecC) {
    sqlite3_result_error_code(ctx, SQLITE_NOMEM);
    return;
  }

  for (int i = 0; i < dimA; i++) {
    vecC[i] = vecA[i] / vecB[i];
  }

  sqlite3_result_blob(ctx, vecC, VEC_TO_BUF_SIZE(dimA), sqlite3_free);
  return;
}

#if defined(_WIN32) && !defined(STATIC_VECDEX)
__declspec(dllexport)
#endif
int sqlite3_vecdex_init(sqlite3 *db, char **pzErrMsg,
#ifndef STATIC_VECDEX
                        const sqlite3_api_routines *pApi
#endif
                        ) {
#ifndef STATIC_VECDEX
  SQLITE_EXTENSION_INIT2(pApi);
#endif
  int rc = SQLITE_OK;

  static const struct {
    const char* zFName;
    int nArg;
    int flags;
    void* pAux;
    void (*xFunc)(sqlite3_context*, int, sqlite3_value**);
  } funcTbl[] = {
    { "vector",          -1, SQLITE_PURE_UTF8, NULL, vectorFunc },
    { "vector0",          1, SQLITE_PURE_UTF8, NULL, vector0Func },
    { "vector_from_json", 1, SQLITE_PURE_UTF8, NULL, vectorFunc },
    { "vector_to_json",   1, SQLITE_PURE_UTF8, NULL, vectorToJsonFunc },
    { "vector_compare",   2, SQLITE_PURE_UTF8, NULL, vectorCompareFunc },
    { "vector_cosim",     2, SQLITE_PURE_UTF8, NULL, vectorCosimFunc },
    { "vector_dist",      2, SQLITE_PURE_UTF8, NULL, vectorDistFunc },
    { "vector_dim",       1, SQLITE_PURE_UTF8, NULL, vectorDimFunc },
    { "vector_avg",       1, SQLITE_PURE_UTF8, NULL, vectorAvgFunc },
    { "vector_norm",      1, SQLITE_PURE_UTF8, NULL, vectorNormFunc },
    { "vector_add",       2, SQLITE_PURE_UTF8, NULL, vectorAddFunc },
    { "vector_sub",       2, SQLITE_PURE_UTF8, NULL, vectorSubFunc },
    { "vector_mul",       2, SQLITE_PURE_UTF8, NULL, vectorMulFunc },
    { "vector_div",       2, SQLITE_PURE_UTF8, NULL, vectorDivFunc },
#ifndef NDEBUG
    { "vector_debug",     1, SQLITE_PURE_UTF8, NULL, vectorDebugFunc },
#endif
  };

  for (int i = 0; i < sizeof(funcTbl) / sizeof(*funcTbl); i++) {
    if ((rc = sqlite3_create_function_v2(
      db,
      funcTbl[i].zFName, funcTbl[i].nArg, funcTbl[i].flags, funcTbl[i].pAux,
      funcTbl[i].xFunc, NULL, NULL, NULL
    )) != SQLITE_OK) {
      *pzErrMsg = sqlite3_mprintf("%s: %s",
                                  funcTbl[i].zFName, sqlite3_errmsg(db));
      return rc;
    }
  }

  return rc;
}
