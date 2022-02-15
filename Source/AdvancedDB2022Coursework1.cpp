#include "AdvancedDB2022Coursework1.hpp"

// **MAIN QUERY FUNCTIONS**

// Implements hash join algorithm
// Smaller relation should be used as the buildSide
const Relation *DBMSImplementationForMarks::hashJoin(const Relation *const probeSide, const Relation *const buildSide) {
    if (probeSide == nullptr || buildSide == nullptr) { return nullptr; }
    // TODO - implement hash join algorithm
    return new Relation;
}

// Returns new sorted relation
const Relation *DBMSImplementationForMarks::sorted(const Relation *relation) {
    // TODO - sorting algorithm
    return relation;
}

// Implements sort-merge join algorithm
// Assumes both relations are sorted and contain unique values
const Relation *DBMSImplementationForMarks::sortMergeJoin(const Relation *const leftSide,
                                                          const Relation *const rightSide) {
    if (leftSide == nullptr || rightSide == nullptr) { return nullptr; }
    auto *result = new Relation; // buffer deleted by runQuery function
    size_t leftIndex = 0, rightIndex = 0;
    while (leftIndex < leftSide->size() && rightIndex < rightSide->size()) {
        const Tuple &leftTuple = leftSide->at(leftIndex);
        const Tuple &rightTuple = rightSide->at(rightIndex);

        const AttributeValue &leftValue = leftTuple.at(joinAttributeIndex);
        const AttributeValue &rightValue = rightTuple.at(joinAttributeIndex);

        if (leftValue < rightValue) { leftIndex++; }
        else if (rightValue < leftValue) { rightIndex++; }
        else {
            Tuple combined(leftTuple.size() + rightTuple.size());
            combined.insert(combined.begin(), leftTuple.begin(), leftTuple.end());
            combined.insert(combined.end(), rightTuple.begin(), rightTuple.end());
            result->push_back(combined);
            leftIndex++;
            rightIndex++;
        }
    }
    return result;
}

// Selects tuples where sum of selected attribute values is greater than the threshold
const Relation *DBMSImplementationForMarks::select(const Relation *const input, const int threshold) {
    if (input == nullptr) { return nullptr; }
    // TODO - implement selection operator
    return new Relation;
}

// Returns sum of product of the selected attribute values
long DBMSImplementationForMarks::sumOfProduct(const Relation *const input) {
    if (input == nullptr) { return 0; }
    // TODO - implement sum of product aggregation
    return 0;
}