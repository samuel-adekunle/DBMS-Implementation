#include "AdvancedDB2022Coursework1.hpp"

// **HELPER FUNCTIONS TO CHECK CONSTRAINTS ON WEAKLY TYPED ATTRIBUTE VALUES**

// General purpose comparison function for weakly typed attribute values
// Returns a pair where the second boolean value determines if the comparison was valid and safe
// The first value returns an int which is left - right for numerical types and strcmp for c-strings
std::pair<int, bool> DBMSImplementationForMarks::comp(const AttributeValue &left, const AttributeValue &right) {
    auto leftType = getAttributeValueType(left);
    auto rightType = getAttributeValueType(right);
    if (leftType != rightType) { return {0, false}; } // Values of different types don't compare
    switch (leftType) {
        case 0: // long
        {
            return {getLongValue(left) - getLongValue(right), true};
        }
        case 1: // double
        {
            // round doubles to the nearest integer (can't equal-compare floating point numbers)
            long leftDoubleValue = lround(getdoubleValue(left));
            long rightDoubleValue = lround(getdoubleValue(right));
            return {leftDoubleValue - rightDoubleValue, true};
        }
        case 2: // char const *
        {
            auto leftStringValue = getStringValue(left);
            auto rightStringValue = getStringValue(right);

            // null values don't equal anything
            if (leftStringValue == nullptr || rightStringValue == nullptr) { return {0, false}; }
            else {
                return {strcmp(leftStringValue, rightStringValue), true};
            }
        }
        default: {
            return {0, false};
        }
    }
}

// Checks equality constraint on two weakly typed attribute values
// Returns a pair of bools where the first is the result of the comparison and the second is its validity
std::pair<bool, bool> DBMSImplementationForMarks::equals(const AttributeValue &left, const AttributeValue &right) {
    const std::pair<int, bool> comparison = comp(left, right);
    return {comparison.first == 0, comparison.second};
}

// Checks less than constraint on two weakly typed attribute values
// Returns a pair of bools where the first is the result of the comparison and the second is its validity
std::pair<bool, bool> DBMSImplementationForMarks::lessThan(const AttributeValue &left, const AttributeValue &right) {
    const std::pair<int, bool> comparison = comp(left, right);
    return {comparison.first < 0, comparison.second};
}

// **MAIN QUERY FUNCTIONS**

// Implements hash join algorithm
// Smaller relation should be used as the buildSide
const Relation *DBMSImplementationForMarks::hashJoin(const Relation *const probeSide, const Relation *const buildSide) {
    if (probeSide == nullptr || buildSide == nullptr) { return nullptr; }
    // TODO - implement hash join algorithm
    return new Relation;
}

// Sorts relation in-place.
void DBMSImplementationForMarks::sort(const Relation *relation) {
    // TODO - implement any efficient sorting algorithm
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

        // TODO - confirm logic here works
        if (lessThan(leftValue, rightValue).first) { leftIndex++; }
        else if (lessThan(rightValue, leftValue).first) { rightIndex++; }
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