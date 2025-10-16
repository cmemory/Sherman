// Copyright 2014 Carnegie Mellon University
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#pragma once    // 保证头文件只被包含一次

#ifndef _ZIPF_H_
#define _ZIPF_H_

#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <stdio.h>
#include <stdint.h>
#include <assert.h>
// #include "util.h"

// 实现的一个基于Zipf分布的随机数生成器

// 用于存储Zipf分布生成器状态的结构体
struct zipf_gen_state {
    // 生成的 items 数量
    uint64_t n;      // number of items (input)
    // Zipf分布的偏度控制参数，通常 (0, 1) 之间。theta 越大，分布越倾斜，越接近“热门”项。
    double theta;    // skewness (input) in (0, 1); or, 0 = uniform, 1 = always
                     // zero
    // 与 theta 相关，用于计算分布的参数
    double alpha;    // only depends on theta
    // 与 theta 相关的阈值，用于控制采样过程中的区分。
    double thres;    // only depends on theta
    // 上一次生成过程中使用的 n 值。
    uint64_t last_n; // last n used to calculate the following
    // n 的浮动值，用于计算。
    double dbl_n;
    // 用于加速计算的一个累积和值
    double zetan;
    // 用于加速采样过程的参数。
    double eta;
    // unsigned short rand_state[3];		// prng state
    // 随机数生成器的状态。
    uint64_t rand_state;
};

// 简单的伪随机数生成器（PRNG），使用给定的 state 更新其状态并返回一个介于 0 和 1 之间的随机浮点数。
// 算法来自于 Java 的 Random 类，通过线性同余法生成随机数。
static double mehcached_rand_d(uint64_t *state) {
    // caution: this is maybe too non-random
    *state = (*state * 0x5deece66dUL + 0xbUL) & ((1UL << 48) - 1);
    return (double)*state / (double)((1UL << 48) - 1);
}

// 近似计算 a^b 的函数，比标准的 pow 函数计算更高效。
// 使用了分数指数和指数平方法（Exponentiation by Squaring）优化了幂的计算。
static double mehcached_pow_approx(double a, double b) {
    // from
    // http://martin.ankerl.com/2012/01/25/optimized-approximative-pow-in-c-and-cpp/

    // calculate approximation with fraction of the exponent
    int e = (int)b;
    union {
        double d;
        int x[2];
    } u = { a };
    u.x[1] =
        (int)((b - (double)e) * (double)(u.x[1] - 1072632447) + 1072632447.);
    u.x[0] = 0;

    // exponentiation by squaring with the exponent's integer part
    // double r = u.d makes everything much slower, not sure why
    // TODO: use popcount?
    double r = 1.;
    while (e) {
        if (e & 1)
            r *= a;
        a *= a;
        e >>= 1;
    }

    return r * u.d;
}

// 初始化 zipf_gen_state 结构体，设置 n、theta、rand_seed 等参数。
// 如果 theta 在 [1., 40.) 范围内会报告错误，因为这个范围的 theta 值不支持。
// 根据 theta 的不同，计算 alpha 和 thres。
static void mehcached_zipf_init(struct zipf_gen_state *state, uint64_t n,
                                double theta, uint64_t rand_seed) {
    assert(n > 0);
    if (theta > 0.992 && theta < 1)
        fprintf(stderr,
                "theta > 0.992 will be inaccurate due to approximation\n");
    if (theta >= 1. && theta < 40.) {
        fprintf(stderr, "theta in [1., 40.) is not supported\n");
        assert(false);
    }
    assert(theta == -1. || (theta >= 0. && theta < 1.) || theta >= 40.);
    assert(rand_seed < (1UL << 48));
    memset(state, 0, sizeof(struct zipf_gen_state));
    state->n = n;
    state->theta = theta;
    if (theta == -1.)
        rand_seed = rand_seed % n;
    else if (theta > 0. && theta < 1.) {
        state->alpha = 1. / (1. - theta);
        state->thres = 1. + mehcached_pow_approx(0.5, theta);
    } else {
        state->alpha = 0.; // unused
        state->thres = 0.; // unused
    }
    state->last_n = 0;
    state->zetan = 0.;
    // state->rand_state[0] = (unsigned short)(rand_seed >> 0);
    // state->rand_state[1] = (unsigned short)(rand_seed >> 16);
    // state->rand_state[2] = (unsigned short)(rand_seed >> 32);
    state->rand_state = rand_seed;
}

// 复制一个已有的 zipf_gen_state 状态，并用新的 rand_seed 重置随机数种子。
static void mehcached_zipf_init_copy(struct zipf_gen_state *state,
                                     const struct zipf_gen_state *src_state,
                                     uint64_t rand_seed) {

    (void)mehcached_zipf_init_copy;
    assert(rand_seed < (1UL << 48));
    memcpy(state, src_state, sizeof(struct zipf_gen_state));
    // state->rand_state[0] = (unsigned short)(rand_seed >> 0);
    // state->rand_state[1] = (unsigned short)(rand_seed >> 16);
    // state->rand_state[2] = (unsigned short)(rand_seed >> 32);
    state->rand_state = rand_seed;
}

// 修改 zipf_gen_state 中的 n 值。
static void mehcached_zipf_change_n(struct zipf_gen_state *state, uint64_t n) {
    (void)mehcached_zipf_change_n;
    state->n = n;
}

// 计算 Zipf 分布中的 ζ 函数（用于加速计算过程）。它是一个累加的和，用来计算 Zipf 分布中的权重。
static double mehcached_zeta(uint64_t last_n, double last_sum, uint64_t n,
                             double theta) {
    if (last_n > n) {
        last_n = 0;
        last_sum = 0.;
    }
    while (last_n < n) {
        last_sum += 1. / mehcached_pow_approx((double)last_n + 1., theta);
        last_n++;
    }
    return last_sum;
}

// 生成下一个符合 Zipf 分布的随机数。
static uint64_t mehcached_zipf_next(struct zipf_gen_state *state) {
    if (state->last_n != state->n) {
        if (state->theta > 0. && state->theta < 1.) {
            state->zetan = mehcached_zeta(state->last_n, state->zetan, state->n,
                                          state->theta);
            state->eta =
                (1. - mehcached_pow_approx(2. / (double)state->n,
                                           1. - state->theta)) /
                (1. - mehcached_zeta(0, 0., 2, state->theta) / state->zetan);
        }
        state->last_n = state->n;
        state->dbl_n = (double)state->n;
    }

    if (state->theta == -1.) {
        uint64_t v = state->rand_state;
        if (++state->rand_state >= state->n)
            state->rand_state = 0;
        return v;
    } else if (state->theta == 0.) {
        double u = mehcached_rand_d(&state->rand_state);
        return (uint64_t)(state->dbl_n * u);
    } else if (state->theta >= 40.) {
        return 0UL;
    } else {
        // from J. Gray et al. Quickly generating billion-record synthetic
        // databases. In SIGMOD, 1994.

        // double u = erand48(state->rand_state);
        double u = mehcached_rand_d(&state->rand_state);
        double uz = u * state->zetan;
        if (uz < 1.)
            return 0UL;
        else if (uz < state->thres)
            return 1UL;
        else
            return (uint64_t)(
                state->dbl_n *
                mehcached_pow_approx(state->eta * (u - 1.) + 1., state->alpha));
    }
}

// 测试给定 theta 值下，使用 Zipf 分布生成的随机数的分布是否接近理论上的 Zipf 分布。
void mehcached_test_zipf(double theta) {

    // “空操作”（no-op）。用于消除未使用函数警告。
    (void)(mehcached_test_zipf);

    double zetan = 0.;
    const uint64_t n = 10000000000UL;
    uint64_t i;

    // 计算 zetan，这是 Zipf 分布的 ζ 函数的近似值。
    for (i = 0; i < n; i++)
        zetan += 1. / pow((double)i + 1., theta);

    struct zipf_gen_state state;
    // 定义一个 zipf_gen_state 类型的变量 state，用于存储 Zipf 分布生成器的状态。
    if (theta < 1. || theta >= 40.)
        mehcached_zipf_init(&state, n, theta, 0);

    uint64_t num_key0 = 0;
    const uint64_t num_samples = 10000000UL;
    // 使用 mehcached_zipf_next 函数生成 num_samples 个 Zipf 分布的随机数。
    // 如果生成 0，则增加 num_key0 计数器。
    // 该过程模拟了根据 Zipf 分布产生的样本，并计算出值为 0 的次数。这个计数有助于评估 Zipf 分布的特性。
    if (theta < 1. || theta >= 40.) {
        for (i = 0; i < num_samples; i++)
            if (mehcached_zipf_next(&state) == 0)
                num_key0++;
    }

    // 0 通常表示生成的样本中没有被选中的项，它反映了 Zipf 分布中较低排名元素的稀有性。
    // 通过统计值为 0 的次数，可以帮助验证生成器的行为是否符合 Zipf 分布的特性，尤其是长尾分布的特性。

    // 输出理论计算的 Zipf 分布的 zetan 值的倒数，这个值是通过直接计算 pow 得到的理论值。
    // 输出 theta 参数以及通过传统的幂函数计算的结果，作为比较的基准。
    printf("theta = %lf; using pow(): %.10lf", theta, 1. / zetan);
    if (theta < 1. || theta >= 40.)
        // 输出近似通过 Zipf 生成器得到的值为 0 的概率。
        // 通过 num_key0（值为 0 的次数）除以总样本数 num_samples 得到
        printf(", using approx-pow(): %.10lf",
               (double)num_key0 / (double)num_samples);
    printf("\n");
}

#endif
