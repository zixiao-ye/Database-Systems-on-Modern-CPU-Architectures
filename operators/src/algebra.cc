#include "moderndbs/algebra.h"
#include <algorithm>
#include <cassert>
#include <functional>
#include <iostream>
#include <string>

namespace moderndbs::iterator_model {
Register Register::from_int(int64_t value) {
    Register reg;
    reg.value = value;

    return reg;
}

Register Register::from_string(const std::string& value) {
    Register reg;
    reg.value = value;

    return reg;
}

Register::Type Register::get_type() const {
    if (value.index() == 0) {
        return Type::INT64;
    } else {
        return Type::CHAR16;
    }
}

int64_t Register::as_int() const { return std::get<int64_t>(value); }

std::string Register::as_string() const { return std::get<std::string>(value); }

uint64_t Register::get_hash() const {
    if (this->get_type() == Type::INT64) {
        return std::hash<int64_t>{}(std::get<int64_t>(value));
    } else {
        return std::hash<std::string>{}(std::get<std::string>(value));
    }
}

bool operator==(const Register& r1, const Register& r2) {
    return r1.value == r2.value;
}

bool operator!=(const Register& r1, const Register& r2) {
    return r1.value != r2.value;
}

bool operator<(const Register& r1, const Register& r2) {
    assert(r1.get_type() == r2.get_type());
    return r1.value < r2.value;
}

bool operator<=(const Register& r1, const Register& r2) {
    assert(r1.get_type() == r2.get_type());
    return r1.value <= r2.value;
}

bool operator>(const Register& r1, const Register& r2) {
    assert(r1.get_type() == r2.get_type());
    return r1.value > r2.value;
}

bool operator>=(const Register& r1, const Register& r2) {
    assert(r1.get_type() == r2.get_type());
    return r1.value >= r2.value;
}

std::vector<Register*> get_result(std::vector<Register>& outputs) {
    std::vector<Register*> results;
    for (size_t i = 0; i < outputs.size(); i++) {
        results.push_back(&outputs[i]);
    }
    return results;
}

Print::Print(Operator& input, std::ostream& stream)
    : UnaryOperator(input), stream(stream) {}

Print::~Print() = default;

void Print::open() {
    input->open();
    regs = input->get_output();
}

bool Print::next() {
    if (input->next()) {
        for (size_t i = 0; i < regs.size(); i++) {
            if (regs[i]->get_type() == Register::Type::INT64) {
                stream << regs[i]->as_int();
            } else {
                stream << regs[i]->as_string();
            }
            if (i + 1 < regs.size()) {
                stream << ",";
            }
        }

        stream << std::endl;
        return true;
    } else {
        return false;
    }
}

void Print::close() { input->close(); }

std::vector<Register*> Print::get_output() {
    // Print has no output
    return {};
}

Projection::Projection(Operator& input, std::vector<size_t> attr_indexes)
    : UnaryOperator(input), attr_indexes(attr_indexes) {}

Projection::~Projection() = default;

void Projection::open() { input->open(); }

bool Projection::next() { return input->next(); }

void Projection::close() { input->close(); }

std::vector<Register*> Projection::get_output() {
    std::vector<Register*> output;
    auto src = input->get_output();
    for (size_t i = 0; i < attr_indexes.size(); i++) {
        output.push_back(src[i]);
    }
    return output;
}

Select::Select(Operator& input, PredicateAttributeInt64 predicate)
    : UnaryOperator(input), attr_left_index(predicate.attr_index),
      predicate_type(predicate.predicate_type) {
    constant = predicate.constant;
    attr_type = AttrType::INT;
}

Select::Select(Operator& input, PredicateAttributeChar16 predicate)
    : UnaryOperator(input), attr_left_index(predicate.attr_index),
      predicate_type(predicate.predicate_type) {
    constant = predicate.constant;
    attr_type = AttrType::STRING;
}

Select::Select(Operator& input, PredicateAttributeAttribute predicate)
    : UnaryOperator(input), attr_left_index(predicate.attr_left_index),
      attr_right_index(predicate.attr_right_index),
      predicate_type(predicate.predicate_type) {
    attr_type = AttrType::INDEX;
}

Select::~Select() = default;

void Select::open() {
    input->open();
    regs = input->get_output();
}

bool Select::next() {
    while (input->next()) {
        switch (predicate_type) {
        case PredicateType::EQ:
            if ((attr_type == AttrType::INT &&
                 regs[attr_left_index]->as_int() ==
                     std::get<int64_t>(constant)) ||
                (attr_type == AttrType::STRING &&
                 regs[attr_left_index]->as_string() ==
                     std::get<std::string>(constant)) ||
                (attr_type == AttrType::INDEX &&
                 *regs[attr_left_index] == *regs[attr_right_index])) {
                return true;
            }
            break;

        case PredicateType::NE:
            if ((attr_type == AttrType::INT &&
                 regs[attr_left_index]->as_int() !=
                     std::get<int64_t>(constant)) ||
                (attr_type == AttrType::STRING &&
                 regs[attr_left_index]->as_string() !=
                     std::get<std::string>(constant)) ||
                (attr_type == AttrType::INDEX &&
                 *regs[attr_left_index] != *regs[attr_right_index])) {
                return true;
            }
            break;

        case PredicateType::LT:
            if ((attr_type == AttrType::INT &&
                 regs[attr_left_index]->as_int() <
                     std::get<int64_t>(constant)) ||
                (attr_type == AttrType::STRING &&
                 regs[attr_left_index]->as_string() <
                     std::get<std::string>(constant)) ||
                (attr_type == AttrType::INDEX &&
                 *regs[attr_left_index] < *regs[attr_right_index])) {
                return true;
            }
            break;

        case PredicateType::LE:
            if ((attr_type == AttrType::INT &&
                 regs[attr_left_index]->as_int() <=
                     std::get<int64_t>(constant)) ||
                (attr_type == AttrType::STRING &&
                 regs[attr_left_index]->as_string() <=
                     std::get<std::string>(constant)) ||
                (attr_type == AttrType::INDEX &&
                 *regs[attr_left_index] <= *regs[attr_right_index])) {
                return true;
            }
            break;

        case PredicateType::GT:
            if ((attr_type == AttrType::INT &&
                 regs[attr_left_index]->as_int() >
                     std::get<int64_t>(constant)) ||
                (attr_type == AttrType::STRING &&
                 regs[attr_left_index]->as_string() >
                     std::get<std::string>(constant)) ||
                (attr_type == AttrType::INDEX &&
                 *regs[attr_left_index] > *regs[attr_right_index])) {
                return true;
            }
            break;

        case PredicateType::GE:
            if ((attr_type == AttrType::INT &&
                 regs[attr_left_index]->as_int() >=
                     std::get<int64_t>(constant)) ||
                (attr_type == AttrType::STRING &&
                 regs[attr_left_index]->as_string() >=
                     std::get<std::string>(constant)) ||
                (attr_type == AttrType::INDEX &&
                 *regs[attr_left_index] >= *regs[attr_right_index])) {
                return true;
            }
            break;

        default:
            break;
        }
    }

    return false;
}

void Select::close() { input->close(); }

std::vector<Register*> Select::get_output() { return input->get_output(); }

Sort::Sort(Operator& input, std::vector<Criterion> criteria)
    : UnaryOperator(input), criteria(criteria) {
    offset = 0;
}

Sort::~Sort() = default;

void Sort::open() {
    input->open();
    inputs = input->get_output();
    outputs.resize(inputs.size());
}

bool Sort::next() {
    // load all tuples
    if (offset == 0) {
        while (input->next()) {
            std::vector<Register> tuple;
            for (size_t i = 0; i < inputs.size(); i++) {
                tuple.push_back(*inputs[i]);
            }
            // std::cout << tuple[0].as_int() << std::endl;
            tuples.push_back(tuple);
        }

        // sort process
        for (auto it = criteria.rbegin(); it != criteria.rend(); it++) {
            auto criterion = *it;
            // std::cout << criterion.desc << std::endl;
            // std::cout << criterion.attr_index << std::endl;
            std::sort(tuples.begin(), tuples.end(),
                      [&criterion](std::vector<Register>& a,
                                   std::vector<Register>& b) {
                          if (criterion.desc) {
                              return a[criterion.attr_index] >
                                     b[criterion.attr_index];
                          } else {
                              return a[criterion.attr_index] <=
                                     b[criterion.attr_index];
                          }
                      });
        }
    }

    if (offset == tuples.size()) {
        return false;
    } else {
        outputs = tuples[offset++];
        // std::cout << outputs[0].as_int() << ",";
        // std::cout << outputs[1].as_int() << ",";
        // std::cout << outputs[2].as_int() << std::endl;
        return true;
    }
}

std::vector<Register*> Sort::get_output() {
    std::vector<Register*> results;
    for (size_t i = 0; i < outputs.size(); i++) {
        // std::cout << outputs[i].as_int() << std::endl;
        results.push_back(&outputs[i]);
    }
    return results;
}

void Sort::close() { input->close(); }

HashJoin::HashJoin(Operator& input_left, Operator& input_right,
                   size_t attr_index_left, size_t attr_index_right)
    : BinaryOperator(input_left, input_right), attr_index_left(attr_index_left),
      attr_index_right(attr_index_right) {}

HashJoin::~HashJoin() = default;

void HashJoin::open() {
    input_left->open();
    input_right->open();
    inputs_left = input_left->get_output();
    inputs_right = input_right->get_output();
    outputs.resize(inputs_left.size() + inputs_right.size());
}

bool HashJoin::next() {
    // build phase
    while (input_left->next()) {
        std::vector<Register> tuple;
        for (size_t i = 0; i < inputs_left.size(); i++) {
            tuple.push_back(*inputs_left[i]);
        }
        // std::cout << inputs_left[attr_index_left]->as_int() << std::endl;
        map_from_attr_to_tuple.emplace(*inputs_left[attr_index_left], tuple);
    }

    // probe phase
    while (input_right->next()) {
        // std::cout << map_from_attr_to_tuple.size() << std::endl;
        auto it = map_from_attr_to_tuple.find(*inputs_right[attr_index_right]);
        if (it != map_from_attr_to_tuple.end()) {
            // find the right table tuple in the left table
            for (size_t i = 0; i < it->second.size(); i++) {
                outputs[i] = it->second[i];
            }
            for (size_t i = 0; i < inputs_right.size(); i++) {
                outputs[it->second.size() + i] = *inputs_right[i];
            }
            return true;
        }
    }

    return false;
}

void HashJoin::close() {
    input_left->close();
    input_right->close();
}

std::vector<Register*> HashJoin::get_output() { return get_result(outputs); }

HashAggregation::HashAggregation(Operator& input,
                                 std::vector<size_t> group_by_attrs,
                                 std::vector<AggrFunc> aggr_funcs)
    : UnaryOperator(input), group_by_attrs(group_by_attrs),
      aggr_funcs(aggr_funcs) {}

HashAggregation::~HashAggregation() = default;

void HashAggregation::open() {
    input->open();
    inputs = input->get_output();
    outputs.resize(group_by_attrs.size() + aggr_funcs.size());
}

bool HashAggregation::next() {
    // hash table build phase
    if (!build) {
        while (input->next()) {
            std::vector<Register> group_attrs;
            for (auto attr : group_by_attrs) {
                group_attrs.push_back(*inputs[attr]);
            }

            auto& attrs = ht[group_attrs];

            // use the first tuple to initialize the aggregation result
            if (attrs.size() == 0) {
                for (auto aggr_func : aggr_funcs) {
                    switch (aggr_func.func) {
                    case AggrFunc::Func::MIN:
                    case AggrFunc::Func::MAX:
                        attrs.push_back(*inputs[aggr_func.attr_index]);
                        break;

                    case AggrFunc::Func::SUM:
                    case AggrFunc::Func::COUNT:
                        attrs.push_back(Register::from_int(0));
                        break;

                    default:
                        break;
                    }
                }
            }

            // go through all tuple and update the results in the aggregation
            // result
            for (size_t i = 0; i < aggr_funcs.size(); i++) {
                auto& aggr_func = aggr_funcs[i];
                auto& attr = attrs[i];
                switch (aggr_func.func) {
                case AggrFunc::Func::MIN:
                    attr = std::min(attr, *inputs[aggr_func.attr_index]);
                    break;
                case AggrFunc::Func::MAX:
                    attr = std::max(attr, *inputs[aggr_func.attr_index]);
                    break;
                case AggrFunc::Func::SUM:
                    attr = Register::from_int(
                        attr.as_int() + inputs[aggr_func.attr_index]->as_int());
                    break;
                case AggrFunc::Func::COUNT:
                    attr = Register::from_int(attr.as_int() + 1);
                    break;

                default:
                    break;
                }
            }
        }
        build = true;
        it = ht.begin();
    }

    if (it != ht.end()) {
        // write results to the outputs registers
        // std::cout << "test" << std::endl;
        for (size_t i = 0; i < group_by_attrs.size(); i++) {
            // std::cout << it->first[i].as_int() << std::endl;
            outputs[i] = it->first[i];
        }
        for (size_t i = 0; i < aggr_funcs.size(); i++) {
            outputs[group_by_attrs.size() + i] = it->second[i];
        }
        it++;
        return true;
    }

    return false;
};

void HashAggregation::close() { input->close(); }

std::vector<Register*> HashAggregation::get_output() {
    return get_result(outputs);
}

Union::Union(Operator& input_left, Operator& input_right)
    : BinaryOperator(input_left, input_right) {}

Union::~Union() = default;

void Union::open() {
    input_left->open();
    inputs_left = input_left->get_output();
    input_right->open();
    inputs_right = input_right->get_output();
    outputs.resize(inputs_left.size());
}

bool Union::next() {
    while (input_left->next()) {
        std::vector<Register> tuple;
        for (size_t i = 0; i < inputs_left.size(); i++) {
            tuple.push_back(*inputs_left[i]);
        }
        auto done = ht.insert(tuple).second;
        if (done) {
            for (size_t i = 0; i < tuple.size(); i++) {
                outputs[i] = tuple[i];
            }
            return true;
        }
    }

    while (input_right->next()) {
        std::vector<Register> tuple;
        for (size_t i = 0; i < inputs_right.size(); i++) {
            tuple.push_back(*inputs_right[i]);
        }
        auto done = ht.insert(tuple).second;
        if (done) {
            for (size_t i = 0; i < tuple.size(); i++) {
                outputs[i] = tuple[i];
            }
            return true;
        }
    }

    return false;
}

std::vector<Register*> Union::get_output() { return get_result(outputs); }

void Union::close() {
    input_left->close();
    input_right->close();
}

UnionAll::UnionAll(Operator& input_left, Operator& input_right)
    : BinaryOperator(input_left, input_right) {}

UnionAll::~UnionAll() = default;

void UnionAll::open() {
    input_left->open();
    inputs_left = input_left->get_output();
    input_right->open();
    inputs_right = input_right->get_output();
    outputs.resize(inputs_left.size());
}

bool UnionAll::next() {
    while (input_left->next()) {
        std::vector<Register> tuple;
        for (size_t i = 0; i < inputs_left.size(); i++) {
            outputs[i] = *inputs_left[i];
        }
        return true;
    }

    while (input_right->next()) {
        std::vector<Register> tuple;
        for (size_t i = 0; i < inputs_right.size(); i++) {
            outputs[i] = *inputs_right[i];
        }
        return true;
    }

    return false;
}

std::vector<Register*> UnionAll::get_output() { return get_result(outputs); }

void UnionAll::close() {
    input_left->close();
    input_right->close();
}

Intersect::Intersect(Operator& input_left, Operator& input_right)
    : BinaryOperator(input_left, input_right) {}

Intersect::~Intersect() = default;

void Intersect::open() {
    input_left->open();
    inputs_left = input_left->get_output();
    input_right->open();
    inputs_right = input_right->get_output();
    outputs.resize(inputs_left.size());
}

bool Intersect::next() {
    while (input_left->next()) {
        std::vector<Register> tuple;
        for (size_t i = 0; i < inputs_left.size(); i++) {
            tuple.push_back(*inputs_left[i]);
        }
        auto done = ht.insert(tuple).second;
    }

    while (input_right->next()) {
        std::vector<Register> tuple;
        for (size_t i = 0; i < inputs_right.size(); i++) {
            tuple.push_back(*inputs_right[i]);
        }
        auto no_duplication = ht_check.insert(tuple).second;
        auto it = ht.find(tuple);
        if (it != ht.end() && no_duplication) {
            for (size_t i = 0; i < tuple.size(); i++) {
                outputs[i] = tuple[i];
            }
            return true;
        }
    }

    return false;
}

std::vector<Register*> Intersect::get_output() { return get_result(outputs); }

void Intersect::close() {
    input_left->close();
    input_right->close();
}

IntersectAll::IntersectAll(Operator& input_left, Operator& input_right)
    : BinaryOperator(input_left, input_right) {}

IntersectAll::~IntersectAll() = default;

void IntersectAll::open() {
    input_left->open();
    inputs_left = input_left->get_output();
    input_right->open();
    inputs_right = input_right->get_output();
    outputs.resize(inputs_left.size());
}

bool IntersectAll::next() {
    while (input_left->next()) {
        std::vector<Register> tuple;
        for (size_t i = 0; i < inputs_left.size(); i++) {
            tuple.push_back(*inputs_left[i]);
        }
        auto done = ht.insert(tuple).second;
    }

    while (input_right->next()) {
        std::vector<Register> tuple;
        for (size_t i = 0; i < inputs_right.size(); i++) {
            tuple.push_back(*inputs_right[i]);
        }

        auto it = ht.find(tuple);
        if (it != ht.end()) {
            for (size_t i = 0; i < tuple.size(); i++) {
                outputs[i] = tuple[i];
            }
            return true;
        }
    }

    return false;
}

std::vector<Register*> IntersectAll::get_output() {
    return get_result(outputs);
}

void IntersectAll::close() {
    input_left->close();
    input_right->close();
}

Except::Except(Operator& input_left, Operator& input_right)
    : BinaryOperator(input_left, input_right) {}

Except::~Except() = default;

void Except::open() {
    input_left->open();
    inputs_left = input_left->get_output();
    input_right->open();
    inputs_right = input_right->get_output();
    outputs.resize(inputs_left.size());
}

bool Except::next() {
    while (input_right->next()) {
        std::vector<Register> tuple;
        for (size_t i = 0; i < inputs_right.size(); i++) {
            tuple.push_back(*inputs_right[i]);
        }
        auto done = ht.insert(tuple).second;
    }

    while (input_left->next()) {
        std::vector<Register> tuple;
        for (size_t i = 0; i < inputs_left.size(); i++) {
            tuple.push_back(*inputs_left[i]);
        }
        auto no_duplication = ht_check.insert(tuple).second;
        auto it = ht.find(tuple);
        if (it == ht.end() && no_duplication) {
            for (size_t i = 0; i < tuple.size(); i++) {
                outputs[i] = tuple[i];
            }
            return true;
        }
    }

    return false;
}

std::vector<Register*> Except::get_output() { return get_result(outputs); }

void Except::close() {
    input_left->close();
    input_right->close();
}

ExceptAll::ExceptAll(Operator& input_left, Operator& input_right)
    : BinaryOperator(input_left, input_right) {}

ExceptAll::~ExceptAll() = default;

void ExceptAll::open() {
    input_left->open();
    inputs_left = input_left->get_output();
    input_right->open();
    inputs_right = input_right->get_output();
    outputs.resize(inputs_left.size());
}

bool ExceptAll::next() {
    while (input_right->next()) {
        std::vector<Register> tuple;
        for (size_t i = 0; i < inputs_right.size(); i++) {
            tuple.push_back(*inputs_right[i]);
        }
        auto& cnt = ht[tuple];
        cnt++;
    }

    while (input_left->next()) {
        std::vector<Register> tuple;
        for (size_t i = 0; i < inputs_left.size(); i++) {
            tuple.push_back(*inputs_left[i]);
        }

        auto it = ht.find(tuple);
        if (it == ht.end()) {
            for (size_t i = 0; i < tuple.size(); i++) {
                outputs[i] = tuple[i];
            }
            return true;
        } else {
            auto& cnt = ht[tuple];
            if (cnt == 0) {
                for (size_t i = 0; i < tuple.size(); i++) {
                    outputs[i] = tuple[i];
                }
                return true;
            }
            cnt--;
        }
    }

    return false;
}

std::vector<Register*> ExceptAll::get_output() { return get_result(outputs); }

void ExceptAll::close() {
    input_left->close();
    input_right->close();
}
} // namespace moderndbs::iterator_model
