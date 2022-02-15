#include "AdvancedDB2022Coursework1.hpp"

// **MAIN QUERY FUNCTIONS**

// Implements hash join algorithm
// Smaller relation should be used as the buildSide
const Relation *DBMSImplementationForMarks::hashJoin(const Relation *const probeSide, const Relation *const buildSide) {
    if (probeSide == nullptr || buildSide == nullptr) { return nullptr; }
    // TODO - implement hash join algorithm
    return new Relation;
}

const Relation *DBMSImplementationForMarks::merge(const Relation *leftSide, const Relation *rightSide) {
    auto *result = new Relation(leftSide->size() + rightSide->size());
    size_t leftIndex = 0, rightIndex = 0;
    while (leftIndex < leftSide->size() && rightIndex < rightSide->size()) {
        const Tuple &leftTuple = leftSide->at(leftIndex);
        const Tuple &rightTuple = rightSide->at(rightIndex);

        const AttributeValue &leftValue = leftTuple.at(joinAttributeIndex);
        const AttributeValue &rightValue = rightTuple.at(joinAttributeIndex);

        if (leftValue < rightValue) {
            result->push_back(leftTuple);
            leftIndex++;
        } else {
            result->push_back(rightTuple);
            rightIndex++;
        }
    }

    while (leftIndex < leftSide->size()) { result->push_back(leftSide->at(leftIndex++)); }
    while (rightIndex < rightSide->size()) { result->push_back(rightSide->at(rightIndex++)); }

    return result;
}

const Relation *
DBMSImplementationForMarks::mergeSort(const Relation::const_iterator &begin, const Relation::const_iterator &end) {
    int dist = std::distance(begin, end);
    if (dist > 1) {
        auto mid = std::next(begin, dist >> 1);
        auto left = mergeSort(begin, mid);
        auto right = mergeSort(mid, end);
        auto combined = merge(left, right);
        // garbage collect
        delete left;
        delete right;
        return combined;
    }
    return new Relation(begin, end);
}

// Returns new sorted relation
const Relation *DBMSImplementationForMarks::sorted(const Relation *relation) {
    if (relation == nullptr) { return nullptr; }
    return mergeSort(relation->cbegin(), relation->cend());
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
    auto *result = new Relation;
    for (const auto &row: *input) {
        long largeOneValue = getLongValue(row[selectAttributeIndex]);
        long largeTwoValue = getLongValue(row[selectAttributeIndex + 3]);
        long smallValue = getLongValue(row[selectAttributeIndex + 6]);
        if (largeOneValue + largeTwoValue + smallValue > threshold) {
            result->push_back(row);
        }
    }
    return result;
}

// Returns sum of product of the selected attribute values
long DBMSImplementationForMarks::sumOfProduct(const Relation *const input) {
    if (input == nullptr) { return 0; }
    long sum = 0;
    for (const auto &row: *input) {
        long largeOneValue = getLongValue(row[sumAttributeIndex]);
        long largeTwoValue = getLongValue(row[sumAttributeIndex + 3]);
        long smallValue = getLongValue(row[sumAttributeIndex + 6]);

        sum += largeOneValue * largeTwoValue * smallValue;
    }
    return sum;
}