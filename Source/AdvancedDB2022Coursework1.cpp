#include "AdvancedDB2022Coursework1.hpp"

// **HELPER FUNCTIONS TO CHECK CONSTRAINTS ON WEAKLY TYPED ATTRIBUTE VALUES**

bool DBMSImplementationForMarks::equals(const AttributeValue &left, const AttributeValue &right) {
    return false;
}

bool DBMSImplementationForMarks::lessThan(const AttributeValue &left, const AttributeValue &right) {
    return false;
}

// **MAIN QUERY FUNCTIONS**

const Relation *DBMSImplementationForMarks::hashJoin(const Relation *probeSide, const Relation *buildSide,
                                                     const size_t attributeIndex) {
    if (probeSide == nullptr || buildSide == nullptr) { return nullptr; }
    // TODO
    return new Relation;
}

const Relation *DBMSImplementationForMarks::sortRelation(const Relation *relation, const size_t attributeIndex) {
    if (relation == nullptr) { return nullptr; }
    // TODO
    return new Relation;
}

const Relation *DBMSImplementationForMarks::sortMergeJoin(const Relation *leftSide, const Relation *rightSide,
                                                          const size_t attributeIndex) {
    if (leftSide == nullptr || rightSide == nullptr) { return nullptr; }
    size_t leftIndex = 0, rightIndex = 0;
    while (leftIndex < leftSide->size() && rightIndex < rightSide->size()) {
        Tuple leftTuple = leftSide->at(leftIndex);
        Tuple rightTuple = rightSide->at(rightIndex);
    }
    return new Relation;
}

const Relation *
DBMSImplementationForMarks::select(const Relation *input, const int threshold, const size_t attributeIndex) {
    if (input == nullptr) { return nullptr; }
    // TODO
    return new Relation;
}

// Returns sumOfProduct of product of c attribute values
long DBMSImplementationForMarks::sumOfProduct(const Relation *input, size_t attributeIndex) {
    if (input == nullptr) { return 0; }
    // TODO
    return 0;
}