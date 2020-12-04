#include "workload.h"
#include "cluster.h"

#include <algorithm>
#include <random>
#include <stdexcept>

/**
 * Zipf (Zeta) random distribution.
 *
 * Implementation taken from drobilla's May 24, 2017 answer to
 * https://stackoverflow.com/questions/9983239/how-to-generate-zipf-distributed-numbers-efficiently
 *
 * That code is referenced with this:
 * "Rejection-inversion to generate variates from monotone discrete
 * distributions", Wolfgang Hörmann and Gerhard Derflinger
 * ACM TOMACS 6.3 (1996): 169-184
 *
 * Note that the Hörmann & Derflinger paper, and the stackoverflow
 * code base incorrectly names the paramater as `q`, when they mean `s`.
 * Thier `q` has nothing to do with the q-series. The names in the code
 * below conform to conventions.
 *
 * Example usage:
 *
 *    std::random_device rd;
 *    std::mt19937 gen(rd());
 *    zipf_distribution<> zipf(300);
 *
 *    for (int i = 0; i < 100; i++)
 *        printf("draw %d %d\n", i, zipf(gen));
 */

template<class IntType = unsigned long, class RealType = double>
class zipf_distribution
{
public:
    typedef IntType result_type;

    static_assert(std::numeric_limits<IntType>::is_integer, "");
    static_assert(!std::numeric_limits<RealType>::is_integer, "");

    /// zipf_distribution(N, s, q)
    /// Zipf distribution for `N` items, in the range `[1,N]` inclusive.
    /// The distribution follows the power-law 1/(n+q)^s with exponent
    /// `s` and Hurwicz q-deformation `q`.
    zipf_distribution(const IntType n=std::numeric_limits<IntType>::max(),
            const RealType s=1.0,
            const RealType q=0.0)
            : n(n)
            , _s(s)
            , _q(q)
            , oms(1.0-s)
            , spole(abs(oms) < epsilon)
            , rvs(spole ? 0.0 : 1.0/oms)
            , H_x1(H(1.5) - h(1.0))
            , H_n(H(n + 0.5))
            , cut(1.0 - H_inv(H(1.5) - h(1.0)))
            , dist(H_x1, H_n)
    {
        if (-0.5 >= q)
            throw std::runtime_error("Range error: Parameter q must be greater than -0.5!");
    }
    void reset() {}

    IntType operator()(std::mt19937& rng)
    {
        while (true)
        {
            const RealType u = dist(rng);
            const RealType x = H_inv(u);
            const IntType  k = std::round(x);
            if (k - x <= cut) return k;
            if (u >= H(k + 0.5) - h(k))
                return k;
        }
    }

    /// Returns the parameter the distribution was constructed with.
    RealType s() const { return _s; }
    /// Returns the Hurwicz q-deformation parameter.
    RealType q() const { return _q; }
    /// Returns the minimum value potentially generated by the distribution.
    result_type min() const { return 1; }
    /// Returns the maximum value potentially generated by the distribution.
    result_type max() const { return n; }


private:
    IntType    n;     ///< Number of elements
    RealType   _s;    ///< Exponent
    RealType   _q;    ///< Deformation
    RealType   oms;   ///< 1-s
    bool       spole; ///< true if s near 1.0
    RealType   rvs;   ///< 1/(1-s)
    RealType   H_x1;  ///< H(x_1)
    RealType   H_n;   ///< H(n)
    RealType   cut;   ///< rejection cut
    std::uniform_real_distribution<RealType> dist;  ///< [H(x_1), H(n)]

    // This provides 16 decimal places of precision,
    // i.e. good to (epsilon)^4 / 24 per expanions log, exp below.
    static constexpr RealType epsilon = 2e-5;

    /** (exp(x) - 1) / x */
    static double
    expxm1bx(const double x)
    {
        if (std::abs(x) > epsilon)
            return std::expm1(x) / x;
        return (1.0 + x/2.0 * (1.0 + x/3.0 * (1.0 + x/4.0)));
    }

    /** log(1 + x) / x */
    static RealType
    log1pxbx(const RealType x)
    {
        if (std::abs(x) > epsilon)
            return std::log1p(x) / x;
        return 1.0 - x * ((1/2.0) - x * ((1/3.0) - x * (1/4.0)));
    }
    /**
     * The hat function h(x) = 1/(x+q)^s
     */
    const RealType h(const RealType x)
    {
        return std::pow(x + _q, -_s);
    }

    /**
     * H(x) is an integral of h(x).
     *     H(x) = [(x+q)^(1-s) - (1+q)^(1-s)] / (1-s)
     * and if s==1 then
     *     H(x) = log(x+q) - log(1+q)
     *
     * Note that the numerator is one less than in the paper
     * order to work with all s. Unfortunately, the naive
     * implementation of the above hits numerical underflow
     * when q is larger than 10 or so, so we split into
     * different regimes.
     *
     * When q != 0, we shift back to what the paper defined:

     *    H(x) = (x+q)^{1-s} / (1-s)
     * and for q != 0 and also s==1, use
     *    H(x) = [exp{(1-s) log(x+q)} - 1] / (1-s)
     */
    const RealType H(const RealType x)
    {
        if (not spole)
            return std::pow(x + _q, oms) / oms;

        const RealType log_xpq = std::log(x + _q);
        return log_xpq * expxm1bx(oms * log_xpq);
    }

    /**
     * The inverse function of H(x).
     *    H^{-1}(y) = [(1-s)y + (1+q)^{1-s}]^{1/(1-s)} - q
     * Same convergence issues as above; two regimes.
     *
     * For s far away from 1.0 use the paper version
     *    H^{-1}(y) = -q + (y(1-s))^{1/(1-s)}
     */
    const RealType H_inv(const RealType y)
    {
        if (not spole)
            return std::pow(y * oms, rvs) - _q;

        return std::exp(y * log1pxbx(oms * y)) - _q;
    }
};

/**
 * Generate a shuffle pattern (i.e., message sizes in the shuffle) based on
 * an imaginary MilliSort process.
 *
 * \param rand_seed
 *      Seed value used to generate the input keys; this ensures that we always
 *      generate the same workload on all nodes.
 * \param num_nodes
 *      Number of nodes in the cluster.
 * \param msg_skew_factor
 *      Zipfian skew factor used to generate the input keys, which controls
 *      the skewness of the message sizes indirectly.
 * \param part_skew_factor
 *      Ratio between the largest partition and the smallest partition.
 * \param skew_input
 *      True means the input partitions will be skewed; false, otherwise.
 * \param skew_output
 *      True means the output partitions will be skewed; false, otherwise.
 * \return
 *      2D double array used to store the (normalized) messages sizes generated
 *      from the MilliSort process (e.g., if the value of m[i][j] is 1.2, then
 *      the message sent from node i to node j will be set to 1.2x of the
 *      average message size).
 */
std::vector<std::vector<double>>
gen_msg_sizes(unsigned rand_seed, int num_nodes, double msg_skew_factor,
        double part_skew_factor, bool skew_input, bool skew_output) {
    std::mt19937 gen(rand_seed);
    zipf_distribution<> zipf(30000, msg_skew_factor);

    std::vector<std::vector<double>> msg_sizes;
    std::vector<std::vector<int>> nodes;
    std::vector<std::tuple<int, int, int>> ext_keys;

    // Compute how far a splitter of the input or output partitions is allowed
    // to move. If the partition skew factor is r (i.e., the largest partition
    // is at most r times as the smallest partition) and the fraction we are
    // allowed to grow/shrink a partition is x, then (1 + 2x)(1 - 2x) = r.
    double offset_ratio = (part_skew_factor - 1) / (2 * part_skew_factor + 2);
    int avg_keys_per_node = num_nodes * 50;
    int offset_lim = avg_keys_per_node * offset_ratio;

    // Construct input partitions (potentially skewed)
    std::vector<int> num_keys(num_nodes, avg_keys_per_node);
    if (skew_input && (offset_lim > 0)) {
        int offset;
        for (int node_id = 0; node_id < num_nodes - 1; node_id++) {
            offset = avg_keys_per_node - offset_lim + gen() % (offset_lim * 2);
            num_keys[node_id] += offset;
            num_keys[node_id + 1] -= offset;
        }
    }
    for (int node_id = 0; node_id < num_nodes; node_id++) {
        nodes.emplace_back();
        msg_sizes.emplace_back(num_nodes);
        for (int key_idx = 0; key_idx < num_keys[node_id]; key_idx++) {
            int num = zipf(gen);
            nodes[node_id].push_back(num);
            ext_keys.emplace_back(num, node_id, key_idx);
        }
    }

    // Construct output partitions.
    int k = -1;
    std::sort(ext_keys.begin(), ext_keys.end());
    std::vector<decltype(ext_keys)::value_type> splitters;
    for (int i = 0; i < num_nodes; i++) {
        k += avg_keys_per_node;
        int offset = 0;
        if (skew_output && (offset_lim > 0)) {
            offset = avg_keys_per_node - offset_lim + gen() % (offset_lim * 2);
        }
        splitters.emplace_back(ext_keys[k + offset]);
    }

    // Compute the message sizes, normalized w.r.t. the average message size.
    for (size_t src = 0; src < nodes.size(); src++) {
        for (size_t key_idx = 0; key_idx < nodes[src].size(); key_idx++) {
            int dst = 0;
            for (auto& sp : splitters) {
                if (std::tie(nodes[src][key_idx], src, key_idx) > sp) {
                    dst++;
                }
            }
            msg_sizes[src][dst] += 1;
        }

        for (size_t dst = 0; dst < nodes.size(); dst++) {
            msg_sizes[src][dst] /= (avg_keys_per_node / num_nodes);
        }
    }
    return msg_sizes;
}

bool
setup_workload_cmd(std::vector<std::string> &words, Cluster &cluster,
        shuffle_op &op)
{
    SetupWorkloadOptions opts;
    if (!opts.parse_args(words)) {
        return false;
    }

    auto msg_sizes = gen_msg_sizes(opts.rand_seed, cluster.num_nodes,
            opts.msg_skew_factor, opts.part_skew_factor, opts.skew_input,
            opts.skew_output);

    // Compute the total number of bytes that will be sent and received in the
    // experiment (so we know how much memory to allocate later).
    int local_rank = cluster.local_rank;
    size_t total_tx_bytes = 0;
    size_t total_rx_bytes = 0;
    for (int peer = 0; peer < cluster.num_nodes; peer++) {
        total_tx_bytes += opts.avg_message_size * msg_sizes[local_rank][peer];
        total_rx_bytes += opts.avg_message_size * msg_sizes[peer][local_rank];
    }

    // Reset member variables in the shuffle object.
    op.num_nodes = cluster.num_nodes;
    op.total_tx_bytes = total_tx_bytes;
    op.total_rx_bytes = total_rx_bytes;
    op.tx_data.reset(new char[total_tx_bytes]);
    op.rx_data.reset(new char[total_rx_bytes]);
    op.next_inmsg_addr.store(op.rx_data.get());

    // Set up the outbound messages and initialize the inbound messages.
    op.out_msgs.clear();
    char* start = op.tx_data.get();
    for (int i = 0; i < cluster.num_nodes; i++) {
        size_t len = msg_sizes[local_rank][i] * opts.avg_message_size;
        op.out_msgs.emplace_back(start, len);
        start += len;
    }
    op.in_msgs.clear();
    op.in_msgs.resize(cluster.num_nodes);

//    printf("total tx bytes: %lu\n", op.total_tx_bytes);
//    printf("total rx bytes: %lu\n", op.total_rx_bytes);
//    for (int i = 0; i < cluster.num_nodes; i++) {
//        printf("len[%2d] = %lu\n", i, op.out_msgs[i].len);
//    }

    return true;
}