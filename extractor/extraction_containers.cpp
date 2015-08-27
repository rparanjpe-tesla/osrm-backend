/*

Copyright (c) 2015, Project OSRM contributors
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

#include "extraction_containers.hpp"
#include "extraction_way.hpp"

#include "../data_structures/coordinate_calculation.hpp"
#include "../data_structures/node_id.hpp"
#include "../data_structures/range_table.hpp"

#include "../util/osrm_exception.hpp"
#include "../util/simple_logger.hpp"
#include "../util/timing_util.hpp"
#include "../util/fingerprint.hpp"

#include <boost/assert.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

#include <stxxl/sort>

#include <chrono>
#include <limits>

ExtractionContainers::ExtractionContainers()
{
    // Check if stxxl can be instantiated
    stxxl::vector<unsigned> dummy_vector;
    name_list.push_back("");
}

ExtractionContainers::~ExtractionContainers()
{
    // FIXME isn't this done implicitly of the stxxl::vectors go out of scope?
    used_node_id_list.clear();
    all_nodes_list.clear();
    all_edges_list.clear();
    name_list.clear();
    restrictions_list.clear();
    way_start_end_id_list.clear();
}

/**
 * Processes the collected data and serializes it.
 * At this point nodes are still referenced by their OSM id.
 *
 * - map start-end nodes of ways to ways used int restrictions to compute compressed
 *   trippe representation
 * - filter nodes list to nodes that are referenced by ways
 * - merge edges with nodes to include location of start/end points and serialize
 *
 */
void ExtractionContainers::PrepareData(const std::string &output_file_name,
                                       const std::string &restrictions_file_name,
                                       const std::string &name_file_name)
{
    try
    {
        std::ofstream file_out_stream;
        file_out_stream.open(output_file_name.c_str(), std::ios::binary);
        const FingerPrint fingerprint = FingerPrint::GetValid();
        file_out_stream.write((char *)&fingerprint, sizeof(FingerPrint));

        PrepareNodes();
        WriteNodes(file_out_stream);
        PrepareEdges();
        WriteEdges(file_out_stream);

        file_out_stream.close();

        PrepareRestrictions();
        WriteRestrictions(restrictions_file_name);

        WriteNames(name_file_name);
    }
    catch (const std::exception &e)
    {
        std::cerr << "Caught Execption:" << e.what() << std::endl;
    }
}

void ExtractionContainers::WriteNames(const std::string& names_file_name) const
{
    std::cout << "[extractor] writing street name index ... " << std::flush;
    TIMER_START(write_name_index);
    boost::filesystem::ofstream name_file_stream(names_file_name, std::ios::binary);

    unsigned total_length = 0;
    std::vector<unsigned> name_lengths;
    for (const std::string &temp_string : name_list)
    {
        const unsigned string_length =
            std::min(static_cast<unsigned>(temp_string.length()), 255u);
        name_lengths.push_back(string_length);
        total_length += string_length;
    }

    // builds and writes the index
    RangeTable<> name_index_range(name_lengths);
    name_file_stream << name_index_range;

    name_file_stream.write((char *)&total_length, sizeof(unsigned));
    // write all chars consecutively
    for (const std::string &temp_string : name_list)
    {
        const unsigned string_length =
            std::min(static_cast<unsigned>(temp_string.length()), 255u);
        name_file_stream.write(temp_string.c_str(), string_length);
    }

    name_file_stream.close();
    TIMER_STOP(write_name_index);
    std::cout << "ok, after " << TIMER_SEC(write_name_index) << "s" << std::endl;
}

void ExtractionContainers::PrepareNodes()
{
    std::cout << "[extractor] Sorting used nodes        ... " << std::flush;
    TIMER_START(sorting_used_nodes);
    stxxl::sort(used_node_id_list.begin(), used_node_id_list.end(), Cmp(), stxxl_memory);
    TIMER_STOP(sorting_used_nodes);
    std::cout << "ok, after " << TIMER_SEC(sorting_used_nodes) << "s" << std::endl;

    std::cout << "[extractor] Erasing duplicate nodes   ... " << std::flush;
    TIMER_START(erasing_dups);
    auto new_end = std::unique(used_node_id_list.begin(), used_node_id_list.end());
    used_node_id_list.resize(new_end - used_node_id_list.begin());
    TIMER_STOP(erasing_dups);
    std::cout << "ok, after " << TIMER_SEC(erasing_dups) << "s" << std::endl;

    std::cout << "[extractor] Building node id map      ... " << std::flush;
    TIMER_START(id_map);
    external_to_internal_node_id_map.reserve(used_node_id_list.size());
    for (NodeID i = 0u; i < used_node_id_list.size(); ++i)
        external_to_internal_node_id_map[used_node_id_list[i]] = i;
    TIMER_STOP(id_map);
    std::cout << "ok, after " << TIMER_SEC(id_map) << "s" << std::endl;

    std::cout << "[extractor] Sorting all nodes         ... " << std::flush;
    TIMER_START(sorting_nodes);
    stxxl::sort(all_nodes_list.begin(), all_nodes_list.end(), ExternalMemoryNodeSTXXLCompare(),
                stxxl_memory);
    TIMER_STOP(sorting_nodes);
    std::cout << "ok, after " << TIMER_SEC(sorting_nodes) << "s" << std::endl;
}

void ExtractionContainers::PrepareEdges()
{
    // Sort edges by start.
    std::cout << "[extractor] Sorting edges by start    ... " << std::flush;
    TIMER_START(sort_edges_by_start);
    stxxl::sort(all_edges_list.begin(), all_edges_list.end(), CmpEdgeByStartID(), stxxl_memory);
    TIMER_STOP(sort_edges_by_start);
    std::cout << "ok, after " << TIMER_SEC(sort_edges_by_start) << "s" << std::endl;

    std::cout << "[extractor] Setting start coords      ... " << std::flush;
    TIMER_START(set_start_coords);
    // Traverse list of edges and nodes in parallel and set start coord
    auto node_iterator = all_nodes_list.begin();
    auto edge_iterator = all_edges_list.begin();
    while (edge_iterator != all_edges_list.end() && node_iterator != all_nodes_list.end())
    {
        if (edge_iterator->result.source < node_iterator->node_id)
        {
            edge_iterator->result.source = SPECIAL_NODEID;
            ++edge_iterator;
            continue;
        }
        if (edge_iterator->result.source > node_iterator->node_id)
        {
            node_iterator++;
            continue;
        }

        // remove loops
        if (edge_iterator->result.source == edge_iterator->result.target)
        {
            edge_iterator->result.source = SPECIAL_NODEID;
            edge_iterator->result.target = SPECIAL_NODEID;
            ++edge_iterator;
            continue;
        }

        BOOST_ASSERT(edge_iterator->result.source == node_iterator->node_id);

        // assign new node id
        auto id_iter = external_to_internal_node_id_map.find(node_iterator->node_id);
        BOOST_ASSERT(id_iter != external_to_internal_node_id_map.end());
        edge_iterator->result.source = id_iter->second;

        edge_iterator->source_coordinate.lat = node_iterator->lat;
        edge_iterator->source_coordinate.lon = node_iterator->lon;
        ++edge_iterator;
    }
    TIMER_STOP(set_start_coords);
    std::cout << "ok, after " << TIMER_SEC(set_start_coords) << "s" << std::endl;

    // Sort Edges by target
    std::cout << "[extractor] Sorting edges by target   ... " << std::flush;
    TIMER_START(sort_edges_by_target);
    stxxl::sort(all_edges_list.begin(), all_edges_list.end(), CmpEdgeByTargetID(),
                stxxl_memory);
    TIMER_STOP(sort_edges_by_target);
    std::cout << "ok, after " << TIMER_SEC(sort_edges_by_target) << "s" << std::endl;

    // Compute edge weights
    std::cout << "[extractor] Computing edge weights    ... " << std::flush;
    TIMER_START(compute_weights);
    node_iterator = all_nodes_list.begin();
    edge_iterator = all_edges_list.begin();
    while (edge_iterator != all_edges_list.end() && node_iterator != all_nodes_list.end())
    {
        // skip all invalid edges
        if (edge_iterator->result.source == SPECIAL_NODEID)
        {
            ++edge_iterator;
            continue;
        }

        if (edge_iterator->result.target < node_iterator->node_id)
        {
            edge_iterator->result.target = SPECIAL_NODEID;
            ++edge_iterator;
            continue;
        }
        if (edge_iterator->result.target > node_iterator->node_id)
        {
            ++node_iterator;
            continue;
        }

        BOOST_ASSERT(edge_iterator->result.target == node_iterator->node_id);
        BOOST_ASSERT(edge_iterator->weight_data.speed >= 0);
        BOOST_ASSERT(edge_iterator->source_coordinate.lat != std::numeric_limits<int>::min());
        BOOST_ASSERT(edge_iterator->source_coordinate.lon != std::numeric_limits<int>::min());

        const double distance = coordinate_calculation::euclidean_distance(
            edge_iterator->source_coordinate.lat, edge_iterator->source_coordinate.lon,
            node_iterator->lat, node_iterator->lon);

        const double weight = [distance](const InternalExtractorEdge::WeightData& data) {
            switch (data.type)
            {
                case InternalExtractorEdge::WeightType::EDGE_DURATION:
                case InternalExtractorEdge::WeightType::WAY_DURATION:
                    return data.duration * 10.;
                    break;
                case InternalExtractorEdge::WeightType::SPEED:
                    return (distance * 10.) / (data.speed / 3.6);
                    break;
                case InternalExtractorEdge::WeightType::INVALID:
                    osrm::exception("invalid weight type");
            }
            return -1.0;
        }(edge_iterator->weight_data);

        auto& edge = edge_iterator->result;
        edge.weight = std::max(1, static_cast<int>(std::floor(weight + .5)));

        // assign new node id
        auto id_iter = external_to_internal_node_id_map.find(node_iterator->node_id);
        BOOST_ASSERT(id_iter != external_to_internal_node_id_map.end());
        edge.target = id_iter->second;

        // orient edges consistently: source id < target id
        // important for multi-edge removal
        if (edge.source > edge.target)
        {
            std::swap(edge.source, edge.target);

            // std::swap does not work with bit-fields
            bool temp = edge.forward;
            edge.forward = edge.backward;
            edge.backward = temp;
        }
        ++edge_iterator;
    }
    TIMER_STOP(compute_weights);
    std::cout << "ok, after " << TIMER_SEC(compute_weights) << "s" << std::endl;

    // Sort edges by start.
    std::cout << "[extractor] Sorting edges by renumbered start ... " << std::flush;
    TIMER_START(sort_edges_by_renumbered_start);
    stxxl::sort(all_edges_list.begin(), all_edges_list.end(), CmpEdgeByStartThenTargetID(), stxxl_memory);
    TIMER_STOP(sort_edges_by_renumbered_start);
    std::cout << "ok, after " << TIMER_SEC(sort_edges_by_renumbered_start) << "s" << std::endl;

    BOOST_ASSERT(all_edges_list.size() > 0);
    for (unsigned i = 0; i < all_edges_list.size();)
    {
        // only invalid edges left
        if (all_edges_list[i].result.source == SPECIAL_NODEID)
        {
            break;
        }
        // skip invalid edges
        if (all_edges_list[i].result.target == SPECIAL_NODEID)
        {
            ++i;
            continue;
        }

        unsigned start_idx = i;
        NodeID source = all_edges_list[i].result.source;
        NodeID target = all_edges_list[i].result.target;

        int min_forward_weight = std::numeric_limits<int>::max();
        int min_backward_weight = std::numeric_limits<int>::max();
        unsigned min_forward_idx = std::numeric_limits<unsigned>::max();
        unsigned min_backward_idx = std::numeric_limits<unsigned>::max();

        // find minimal edge in both directions
        while (all_edges_list[i].result.source == source &&
               all_edges_list[i].result.target == target)
        {
            if (all_edges_list[i].result.forward && all_edges_list[i].result.weight < min_forward_weight)
            {
                min_forward_idx = i;
            }
            if (all_edges_list[i].result.backward && all_edges_list[i].result.weight < min_backward_weight)
            {
                min_backward_idx = i;
            }

            // this also increments the outer loop counter!
            i++;
        }

        BOOST_ASSERT(min_forward_idx == std::numeric_limits<unsigned>::max() || min_forward_idx < i);
        BOOST_ASSERT(min_backward_idx == std::numeric_limits<unsigned>::max() || min_backward_idx < i);
        BOOST_ASSERT(min_backward_idx != std::numeric_limits<unsigned>::max() ||
                     min_forward_idx != std::numeric_limits<unsigned>::max());

        if (min_backward_idx == min_forward_idx)
        {
            all_edges_list[min_forward_idx].result.is_split = false;
            all_edges_list[min_forward_idx].result.forward = true;
            all_edges_list[min_forward_idx].result.backward = true;
        }
        else
        {
            bool has_forward = min_forward_idx != std::numeric_limits<unsigned>::max();
            bool has_backward = min_backward_idx != std::numeric_limits<unsigned>::max();
            if (has_forward)
            {
                all_edges_list[min_forward_idx].result.forward = true;
                all_edges_list[min_forward_idx].result.backward = false;
                all_edges_list[min_forward_idx].result.is_split = has_backward;
            }
            if (has_backward)
            {
                std::swap(all_edges_list[min_backward_idx].result.source,
                          all_edges_list[min_backward_idx].result.target);
                all_edges_list[min_backward_idx].result.forward = true;
                all_edges_list[min_backward_idx].result.backward = false;
                all_edges_list[min_backward_idx].result.is_split = has_forward;
            }
        }

        // invalidate all unused edges
        for (unsigned j = start_idx; j < i; j++)
        {
            if (j == min_forward_idx || j == min_backward_idx)
            {
                continue;
            }
            all_edges_list[j].result.source = SPECIAL_NODEID;
            all_edges_list[j].result.target = SPECIAL_NODEID;
        }
    }
}

void ExtractionContainers::WriteEdges(std::ofstream& file_out_stream) const
{
    std::cout << "[extractor] Writing used egdes       ... " << std::flush;
    TIMER_START(write_edges);
    // Traverse list of edges and nodes in parallel and set target coord
    unsigned number_of_used_edges = 0;

    auto start_position = file_out_stream.tellp();
    file_out_stream.write((char *)&number_of_used_edges, sizeof(unsigned));

    for (const auto& edge : all_edges_list)
    {
        if (edge.result.source == SPECIAL_NODEID || edge.result.target == SPECIAL_NODEID)
        {
            continue;
        }

        file_out_stream.write((char*) &edge.result, sizeof(NodeBasedEdge));
        number_of_used_edges++;
    }
    TIMER_STOP(write_edges);
    std::cout << "ok, after " << TIMER_SEC(write_edges) << "s" << std::endl;

    std::cout << "[extractor] setting number of edges   ... " << std::flush;
    file_out_stream.seekp(start_position);
    file_out_stream.write((char *)&number_of_used_edges, sizeof(unsigned));
    std::cout << "ok" << std::endl;

    SimpleLogger().Write() << "Processed " << number_of_used_edges << " edges";
}

void ExtractionContainers::WriteNodes(std::ofstream& file_out_stream) const
{
    unsigned number_of_used_nodes = 0;
    // write dummy value, will be overwritten later
    file_out_stream.write((char *)&number_of_used_nodes, sizeof(unsigned));
    std::cout << "[extractor] Confirming/Writing used nodes     ... " << std::flush;
    TIMER_START(write_nodes);
    // identify all used nodes by a merging step of two sorted lists
    auto node_iterator = all_nodes_list.begin();
    auto node_id_iterator = used_node_id_list.begin();
    while (node_id_iterator != used_node_id_list.end() && node_iterator != all_nodes_list.end())
    {
        if (*node_id_iterator < node_iterator->node_id)
        {
            ++node_id_iterator;
            continue;
        }
        if (*node_id_iterator > node_iterator->node_id)
        {
            ++node_iterator;
            continue;
        }
        BOOST_ASSERT(*node_id_iterator == node_iterator->node_id);

        file_out_stream.write((char *)&(*node_iterator), sizeof(ExternalMemoryNode));

        ++number_of_used_nodes;
        ++node_id_iterator;
        ++node_iterator;
    }
    TIMER_STOP(write_nodes);
    std::cout << "ok, after " << TIMER_SEC(write_nodes) << "s" << std::endl;

    std::cout << "[extractor] setting number of nodes   ... " << std::flush;
    std::ios::pos_type previous_file_position = file_out_stream.tellp();
    file_out_stream.seekp(std::ios::beg + sizeof(FingerPrint));
    file_out_stream.write((char *)&number_of_used_nodes, sizeof(unsigned));
    file_out_stream.seekp(previous_file_position);
    std::cout << "ok" << std::endl;

    SimpleLogger().Write() << "Processed " << number_of_used_nodes << " nodes";
}

void ExtractionContainers::WriteRestrictions(const std::string& path) const
{
    // serialize restrictions
    std::ofstream restrictions_out_stream;
    unsigned written_restriction_count = 0;
    restrictions_out_stream.open(path.c_str(), std::ios::binary);
    const FingerPrint fingerprint = FingerPrint::GetValid();
    restrictions_out_stream.write((char *)&fingerprint, sizeof(FingerPrint));
    const auto count_position = restrictions_out_stream.tellp();
    restrictions_out_stream.write((char *)&written_restriction_count, sizeof(unsigned));

    for (const auto &restriction_container : restrictions_list)
    {
        if (((SPECIAL_NODEID != restriction_container.restriction.from.node &&
            SPECIAL_NODEID != restriction_container.restriction.via.node &&
            SPECIAL_NODEID != restriction_container.restriction.to.node) ||
            (SPECIAL_EDGEID != restriction_container.restriction.from.way &&
            SPECIAL_EDGEID != restriction_container.restriction.via.way &&
            SPECIAL_EDGEID != restriction_container.restriction.to.way)) &&
            !restriction_container.restriction.flags.ignore_write)
        {
            restrictions_out_stream.write((char *)&(restriction_container.restriction),
                                          sizeof(TurnRestriction));
            ++written_restriction_count;
        }
    }
    restrictions_out_stream.seekp(count_position);
    restrictions_out_stream.write((char *)&written_restriction_count, sizeof(unsigned));
    restrictions_out_stream.close();
    SimpleLogger().Write() << "usable restrictions: " << written_restriction_count;
}

/**
* Prepares restrictions for writing to .restrictions file. Unfolds via-way restrictions into multiple
* restrictions (one for each node in the via way and two for the from-way and to-way). For example, a via-way
* restriction a->b->c with nodes: a: [0,1,2], b:[2,3,4], c:[4,5,6] will be split into 3 restrictions (in order):
*
* Restriction# |  from (node) | to (node)  | via (node)
* -------------|--------------|------------|-----------
*            0 |       1      |     ---    |      2
*            1 |      ---     |      3     |     ---
*            2 |      ---     |      5     |      4
*
* '---' indicates the SPECIAL_NODEID, useful for when we parse through an unfolded via-way turn restriction.
*/
void ExtractionContainers::PrepareRestrictions()
{
    std::cout << "[extractor] Sorting used ways         ... " << std::flush;
    TIMER_START(sort_ways);
    stxxl::sort(way_start_end_id_list.begin(), way_start_end_id_list.end(),
                FirstAndLastSegmentOfWayStxxlCompare(), stxxl_memory);
    TIMER_STOP(sort_ways);
    std::cout << "ok, after " << TIMER_SEC(sort_ways) << "s" << std::endl;

    std::cout << "[extractor] Sorting " << restrictions_list.size()
              << " restriction. by from... " << std::flush;
    TIMER_START(sort_restrictions);
    stxxl::sort(restrictions_list.begin(), restrictions_list.end(),
                CmpRestrictionContainerByFrom(), stxxl_memory);
    TIMER_STOP(sort_restrictions);
    std::cout << "ok, after " << TIMER_SEC(sort_restrictions) << "s" << std::endl;

    std::cout << "[extractor] Fixing restriction starts ... " << std::flush;
    TIMER_START(fix_restriction_starts);
    auto restrictions_iterator = restrictions_list.begin();
    auto way_start_and_end_iterator = way_start_end_id_list.cbegin();

    while (way_start_and_end_iterator != way_start_end_id_list.cend() &&
           restrictions_iterator != restrictions_list.end())
    {
        // Skip via-way restrictions (we'll deal with these later)
        if (restrictions_iterator->restriction.flags.uses_via_way) {
            continue;
        }

        if (way_start_and_end_iterator->way_id < restrictions_iterator->restriction.from.way)
        {
            ++way_start_and_end_iterator;
            continue;
        }

        if (way_start_and_end_iterator->way_id > restrictions_iterator->restriction.from.way)
        {
            SimpleLogger().Write(LogLevel::logDEBUG) << "Restriction references invalid way: " << restrictions_iterator->restriction.from.way;
            restrictions_iterator->restriction.from.node = SPECIAL_NODEID;
            ++restrictions_iterator;
            continue;
        }

        BOOST_ASSERT(way_start_and_end_iterator->way_id ==
                     restrictions_iterator->restriction.from.way);
        // we do not remap the via id yet, since we will need it for the to node as well
        const NodeID via_node_id = restrictions_iterator->restriction.via.node;

        // check if via is actually valid, if not invalidate
        auto via_id_iter = external_to_internal_node_id_map.find(via_node_id);
        if(via_id_iter == external_to_internal_node_id_map.end())
        {
            SimpleLogger().Write(LogLevel::logDEBUG) << "Restriction references invalid node: " << restrictions_iterator->restriction.via.node;
            restrictions_iterator->restriction.via.node = SPECIAL_NODEID;
            ++restrictions_iterator;
            continue;
        }

        if (way_start_and_end_iterator->first_segment_source_id == via_node_id)
        {
            // assign new from node id
            auto id_iter = external_to_internal_node_id_map.find(
                    way_start_and_end_iterator->first_segment_target_id);
            BOOST_ASSERT(id_iter != external_to_internal_node_id_map.end());
            restrictions_iterator->restriction.from.node = id_iter->second;
        }
        else if (way_start_and_end_iterator->last_segment_target_id == via_node_id)
        {
            // assign new from node id
            auto id_iter = external_to_internal_node_id_map.find(
                    way_start_and_end_iterator->last_segment_source_id);
            BOOST_ASSERT(id_iter != external_to_internal_node_id_map.end());
            restrictions_iterator->restriction.from.node = id_iter->second;
        }
        ++restrictions_iterator;
    }

    TIMER_STOP(fix_restriction_starts);
    std::cout << "ok, after " << TIMER_SEC(fix_restriction_starts) << "s" << std::endl;

    std::cout << "[extractor] Sorting restrictions. by to  ... " << std::flush;
    TIMER_START(sort_restrictions_to);
    stxxl::sort(restrictions_list.begin(), restrictions_list.end(),
                CmpRestrictionContainerByTo(), stxxl_memory);
    TIMER_STOP(sort_restrictions_to);
    std::cout << "ok, after " << TIMER_SEC(sort_restrictions_to) << "s" << std::endl;

    std::cout << "[extractor] Fixing restriction ends   ... " << std::flush;
    TIMER_START(fix_restriction_ends);
    restrictions_iterator = restrictions_list.begin();
    way_start_and_end_iterator = way_start_end_id_list.cbegin();

    while (way_start_and_end_iterator != way_start_end_id_list.cend() &&
           restrictions_iterator != restrictions_list.end())
    {
        // Skip via-way restrictions (we'll deal with these later)
        if (restrictions_iterator->restriction.flags.uses_via_way) {
            continue;
        }

        if (way_start_and_end_iterator->way_id < restrictions_iterator->restriction.to.way)
        {
            ++way_start_and_end_iterator;
            continue;
        }
        if (restrictions_iterator->restriction.from.node == SPECIAL_NODEID ||
            restrictions_iterator->restriction.via.node == SPECIAL_NODEID)
        {
            ++restrictions_iterator;
            continue;
        }
        if (way_start_and_end_iterator->way_id > restrictions_iterator->restriction.to.way)
        {
            SimpleLogger().Write(LogLevel::logDEBUG) << "Restriction references invalid way: " << restrictions_iterator->restriction.to.way;
            restrictions_iterator->restriction.to.way = SPECIAL_NODEID;
            ++restrictions_iterator;
            continue;
        }
        BOOST_ASSERT(way_start_and_end_iterator->way_id ==
                     restrictions_iterator->restriction.to.way);
        const NodeID via_node_id = restrictions_iterator->restriction.via.node;

        // assign new via node id
        auto via_id_iter = external_to_internal_node_id_map.find(via_node_id);
        BOOST_ASSERT(via_id_iter != external_to_internal_node_id_map.end());
        restrictions_iterator->restriction.via.node = via_id_iter->second;

        if (way_start_and_end_iterator->first_segment_source_id == via_node_id)
        {
            auto to_id_iter = external_to_internal_node_id_map.find(
                    way_start_and_end_iterator->first_segment_target_id);
            BOOST_ASSERT(to_id_iter != external_to_internal_node_id_map.end());
            restrictions_iterator->restriction.to.node = to_id_iter->second;
        }
        else if (way_start_and_end_iterator->last_segment_target_id == via_node_id)
        {
            auto to_id_iter = external_to_internal_node_id_map.find(
                    way_start_and_end_iterator->last_segment_source_id);
            BOOST_ASSERT(to_id_iter != external_to_internal_node_id_map.end());
            restrictions_iterator->restriction.to.node = to_id_iter->second;
        }
        ++restrictions_iterator;
    }
    TIMER_STOP(fix_restriction_ends);
    std::cout << "ok, after " << TIMER_SEC(fix_restriction_ends) << "s" << std::endl;

    std::cout << "[extractor] Sorting restrictions. by via way ... " << std::flush;
    TIMER_START(sort_restrictions_via);
    stxxl::sort(restrictions_list.begin(), restrictions_list.end(),
                CmpRestrictionContainerByVia(), stxxl_memory);
    TIMER_STOP(sort_restrictions_via);
    std::cout << "ok, after " << TIMER_SEC(sort_restrictions_via) << "s" << std::endl;

    std::cout << "[extractor] Unfolding via-way restrictions  ... " << std::flush;
    TIMER_START(unfold_via_ways);
    int unfolded_restrictions_count = 0;
    restrictions_iterator = restrictions_list.begin();
    auto wse_iterator_via = way_start_end_id_list.cbegin();

    EdgeID via_way_id;
    while (wse_iterator_via != way_start_end_id_list.cend() &&
           restrictions_iterator != restrictions_list.end())
    {
        via_way_id = wse_iterator_via->way_id;
        if (!restrictions_iterator->restriction.flags.uses_via_way) {
            continue;
        }

        if (via_way_id < restrictions_iterator->restriction.via.way)
        {
            ++wse_iterator_via;
            continue;
        }

        if (via_way_id > restrictions_iterator->restriction.via.way)
        {
            SimpleLogger().Write(LogLevel::logDEBUG) << "Restriction references invalid via-way: " << restrictions_iterator->restriction.via.way;
            ++restrictions_iterator;
            continue;
        }

        BOOST_ASSERT(via_way_id ==
                     restrictions_iterator->restriction.via.way);

        std::vector<InternalExtractorEdge> from_edges = way_to_edges_map[restrictions_iterator->restriction.from.way];
        std::vector<InternalExtractorEdge> via_edges  = way_to_edges_map[restrictions_iterator->restriction.via.way];
        std::vector<InternalExtractorEdge> to_edges   = way_to_edges_map[restrictions_iterator->restriction.to.way];

        NodeID from_via_node_id = findConnectingNodeID(from_edges, via_edges);
        NodeID via_to_node_id = findConnectingNodeID(via_edges, to_edges);

        if (from_via_node_id == SPECIAL_NODEID || via_to_node_id == SPECIAL_NODEID)
        {
            SimpleLogger().Write(LogLevel::logDEBUG) << "Invalid via-way restriction: " << restrictions_iterator->restriction.via.way;
            ++restrictions_iterator;
            continue;
        }

        // Ensure the connecting nodes connect all the ways correctly
        BOOST_ASSERT(from_edges.front().result.source == from_via_node_id || from_edges.back().result.target == from_via_node_id);
        BOOST_ASSERT(via_edges.front().result.source == from_via_node_id || via_edges.back().result.target == from_via_node_id);
        BOOST_ASSERT(via_edges.front().result.source == via_to_node_id || via_edges.back().result.target == via_to_node_id);
        BOOST_ASSERT(to_edges.front().result.source == via_to_node_id || to_edges.back().result.target == via_to_node_id);

        InputRestrictionContainer from_restriction;
        InputRestrictionContainer to_restriction;
        from_restriction.restriction.flags.uses_via_way = true;
        to_restriction.restriction.flags.uses_via_way = true;

        if (from_edges.front().result.source == from_via_node_id) {
            from_restriction.restriction.from.node = from_edges.front().result.target;
            from_restriction.restriction.to.node = from_via_node_id;
        }
        else {
            from_restriction.restriction.from.node = from_edges.back().result.source;
            from_restriction.restriction.to.node = from_via_node_id;
        }

        if (to_edges.front().result.source == via_to_node_id) {
            to_restriction.restriction.from.node = via_to_node_id;
            to_restriction.restriction.to.node = to_edges.front().result.target;
        }
        else {
            to_restriction.restriction.from.node = via_to_node_id;
            to_restriction.restriction.to.node = to_edges.back().result.source;
        }

        // Write the unfolded restriction to the restrictions_list
        restrictions_list.push_back(from_restriction);

        InputRestrictionContainer via_restriction;

        // Traverse forwards
        if (via_edges.front().result.source == from_via_node_id) {
            for (auto edge_iterator = via_edges.begin(); edge_iterator != via_edges.end(); ++edge_iterator) {
                if (edge_iterator->result.target == via_to_node_id) {
                    break;
                }
                via_restriction.restriction.via.node = edge_iterator->result.target;
                restrictions_list.push_back(via_restriction);
            }
        }
        // Traverse backwards
        else {
            for (auto edge_iterator = via_edges.end()-1; edge_iterator >= via_edges.begin(); --edge_iterator) {
                if (edge_iterator->result.target == from_via_node_id) {
                    break;
                }
                via_restriction.restriction.via.node = edge_iterator->result.target;
                restrictions_list.push_back(via_restriction);
            }
        }

        restrictions_list.push_back(to_restriction);
        ++unfolded_restrictions_count;
    }

    TIMER_STOP(unfold_via_ways);
    std::cout << "ok, after " << TIMER_SEC(unfold_via_ways) << "s" << std::endl;

    std::cout << "Number of unfolded restrictions: " << unfolded_restrictions_count << std::endl;
}

// Finds node ID that connects the end points of two ways. If no node connects the ends of the ways, this returns
// the SPECIAL_NODEID
NodeID ExtractionContainers::findConnectingNodeID(const std::vector<InternalExtractorEdge> &way_edges1, const std::vector<InternalExtractorEdge> &way_edges2) {
    if ((way_edges1.front().result.source == way_edges2.front().result.source) ||
        (way_edges1.front().result.source == way_edges2.back().result.target))
    {
        return way_edges1.front().result.source;
    }
    else if ((way_edges1.back().result.target == way_edges2.back().result.target) ||
             (way_edges1.back().result.target == way_edges2.front().result.source))
    {
        return way_edges1.back().result.target;
    }
    return SPECIAL_NODEID;
}
