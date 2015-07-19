#ifndef _BGDWRITER_H__
#define _BGDWRITER_H__

#include <filter.h>
#include "bgdutils.h"

ErrCode write_object(char *file, void *obj, OpType optype, DataFormat format);

int read_object(char *file, OpType optype, DataFormat format, void *data);

#endif  // _BGDWRITER_H__
