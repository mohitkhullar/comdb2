#if defined(__APPLE__) && defined(COMDB2_WITH_METAL_SORT)

#import <Metal/Metal.h>
#import <Foundation/Foundation.h>
#include <stdint.h>
#include <dispatch/dispatch.h>

#include "vdbesort_metal.h"

static id<MTLDevice> g_device = nil;
static id<MTLCommandQueue> g_queue = nil;
static id<MTLComputePipelineState> g_pipeline = nil;
static int g_metal_available = 0;
static dispatch_once_t g_init_token;

static const char *g_shader_source =
    "#include <metal_stdlib>\n"
    "using namespace metal;\n"
    "kernel void bitonic_sort_step(\n"
    "    device const long *keys [[buffer(0)]],\n"
    "    device uint *indices    [[buffer(1)]],\n"
    "    constant uint &n        [[buffer(2)]],\n"
    "    constant uint &k        [[buffer(3)]],\n"
    "    constant uint &j        [[buffer(4)]],\n"
    "    constant uint &desc     [[buffer(5)]],\n"
    "    uint tid [[thread_position_in_grid]])\n"
    "{\n"
    "    uint ixj = tid ^ j;\n"
    "    if (ixj <= tid || tid >= n || ixj >= n) return;\n"
    "    uint idx_l = indices[tid];\n"
    "    uint idx_r = indices[ixj];\n"
    "    long val_l = keys[idx_l];\n"
    "    long val_r = keys[idx_r];\n"
    "    bool asc = ((tid & k) == 0) ^ (desc != 0);\n"
    "    if (asc ? (val_l > val_r) : (val_l < val_r)) {\n"
    "        indices[tid] = idx_r;\n"
    "        indices[ixj] = idx_l;\n"
    "    }\n"
    "}\n";

int vdbeSorterMetalInit(void)
{
    dispatch_once(&g_init_token, ^{
        g_device = MTLCreateSystemDefaultDevice();
        if (!g_device) return;

        NSError *error = nil;
        NSString *src = [NSString stringWithUTF8String:g_shader_source];
        id<MTLLibrary> lib = [g_device newLibraryWithSource:src options:nil error:&error];
        if (!lib) {
            g_device = nil;
            return;
        }

        id<MTLFunction> fn = [lib newFunctionWithName:@"bitonic_sort_step"];
        if (!fn) {
            g_device = nil;
            return;
        }

        g_pipeline = [g_device newComputePipelineStateWithFunction:fn error:&error];
        if (!g_pipeline) {
            g_device = nil;
            return;
        }

        g_queue = [g_device newCommandQueue];
        if (!g_queue) {
            g_pipeline = nil;
            g_device = nil;
            return;
        }

        g_metal_available = 1;
    });

    return g_metal_available ? 0 : -1;
}

static uint32_t next_power_of_2(uint32_t v)
{
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v++;
    return v;
}

int vdbeSorterMetalSort(const int64_t *pKeys, uint32_t *pIndices,
                        uint32_t nRecord, int bDesc)
{
    if (!g_metal_available || nRecord < 2) return -1;

    uint32_t nPadded = next_power_of_2(nRecord);

    size_t keyBufSize = (size_t)nPadded * sizeof(int64_t);
    size_t idxBufSize = (size_t)nPadded * sizeof(uint32_t);

    if (keyBufSize > [g_device maxBufferLength] ||
        idxBufSize > [g_device maxBufferLength]) {
        return -1;
    }

    id<MTLBuffer> keyBuf = [g_device newBufferWithLength:keyBufSize
                                                options:MTLResourceStorageModeShared];
    id<MTLBuffer> idxBuf = [g_device newBufferWithLength:idxBufSize
                                                options:MTLResourceStorageModeShared];
    if (!keyBuf || !idxBuf) return -1;

    int64_t *gpuKeys = (int64_t *)[keyBuf contents];
    uint32_t *gpuIndices = (uint32_t *)[idxBuf contents];

    memcpy(gpuKeys, pKeys, (size_t)nRecord * sizeof(int64_t));
    int64_t sentinel = bDesc ? INT64_MIN : INT64_MAX;
    for (uint32_t i = nRecord; i < nPadded; i++) {
        gpuKeys[i] = sentinel;
    }

    for (uint32_t i = 0; i < nPadded; i++) {
        gpuIndices[i] = i;
    }

    uint32_t descVal = bDesc ? 1 : 0;
    id<MTLBuffer> nBuf = [g_device newBufferWithBytes:&nPadded length:sizeof(uint32_t)
                                             options:MTLResourceStorageModeShared];
    id<MTLBuffer> descBuf = [g_device newBufferWithBytes:&descVal length:sizeof(uint32_t)
                                                options:MTLResourceStorageModeShared];

    NSUInteger threadGroupSize = g_pipeline.maxTotalThreadsPerThreadgroup;
    if (threadGroupSize > nPadded) threadGroupSize = nPadded;

    id<MTLCommandBuffer> cmdBuf = [g_queue commandBuffer];
    if (!cmdBuf) return -1;

    for (uint32_t k = 2; k <= nPadded; k <<= 1) {
        for (uint32_t j = k >> 1; j > 0; j >>= 1) {
            id<MTLComputeCommandEncoder> enc = [cmdBuf computeCommandEncoder];
            if (!enc) return -1;

            [enc setComputePipelineState:g_pipeline];
            [enc setBuffer:keyBuf offset:0 atIndex:0];
            [enc setBuffer:idxBuf offset:0 atIndex:1];
            [enc setBuffer:nBuf offset:0 atIndex:2];
            [enc setBytes:&k length:sizeof(uint32_t) atIndex:3];
            [enc setBytes:&j length:sizeof(uint32_t) atIndex:4];
            [enc setBuffer:descBuf offset:0 atIndex:5];

            MTLSize gridSize = MTLSizeMake(nPadded, 1, 1);
            MTLSize groupSize = MTLSizeMake(threadGroupSize, 1, 1);
            [enc dispatchThreads:gridSize threadsPerThreadgroup:groupSize];
            [enc endEncoding];
        }
    }

    [cmdBuf commit];
    [cmdBuf waitUntilCompleted];

    if ([cmdBuf error]) return -1;

    uint32_t out = 0;
    for (uint32_t i = 0; i < nPadded && out < nRecord; i++) {
        uint32_t idx = gpuIndices[i];
        if (idx < nRecord) {
            pIndices[out++] = idx;
        }
    }

    if (out != nRecord) return -1;

    return 0;
}

void vdbeSorterMetalShutdown(void)
{
    g_pipeline = nil;
    g_queue = nil;
    g_device = nil;
    g_metal_available = 0;
}

#endif
