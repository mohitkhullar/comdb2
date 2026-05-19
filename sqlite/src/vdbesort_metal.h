#ifndef VDBESORT_METAL_H
#define VDBESORT_METAL_H

#if defined(__APPLE__) && defined(COMDB2_WITH_METAL_SORT)

#include <stdint.h>

int vdbeSorterMetalInit(void);
int vdbeSorterMetalSort(const int64_t *pKeys, uint32_t *pIndices,
                        uint32_t nRecord, int bDesc);
void vdbeSorterMetalShutdown(void);

#endif
#endif
