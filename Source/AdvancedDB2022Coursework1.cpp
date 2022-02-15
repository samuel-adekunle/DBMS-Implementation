#include "AdvancedDB2022Coursework1.hpp"

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

// Checks less than constraint on two weakly typed attribute values
// Returns a pair of bools where the first is the result of the comparison and the second is its validity
bool DBMSImplementationForMarks::lessThan(const AttributeValue &left, const AttributeValue &right) {
    auto[value, valid] = comp(left, right);
    if (valid) return value < 0;
    if (left.index() == 2 && getStringValue(left) == nullptr) { return false; }
    if (right.index() == 2 && getStringValue(right) == nullptr) { return true; }
    return left.index() < right.index();
}

// **MAIN QUERY FUNCTIONS**

// TODO - implement
// Implements hash join algorithm
// Smaller relation should be used as the buildSide
const Relation *DBMSImplementationForMarks::hashJoin(const Relation *const probeSide, const Relation *const buildSide) {
    if (probeSide == nullptr || buildSide == nullptr) { return nullptr; }
    else {
        const size_t hashTableSize = buildSide->size() * 2;
        const long key = lround(buildSide->size() * 0.75); // TODO - change to next prime
        auto *hashTable = new Relation(hashTableSize);
        auto *result = new Relation();

        auto hash = [&key](const AttributeValue &value) {
            return getLongValue(value) % key; // TODO
        };

        auto nextSlot = [&hashTableSize](const unsigned long slot) {
            return (slot + 1) % hashTableSize;
        };


        // Build
        for (const auto &buildTuple: *buildSide) {
            const AttributeValue &buildValue = buildTuple.at(joinAttributeIndex);
            unsigned long hashValue = hash(buildValue);
            while (!hashTable->at(hashValue).empty()) {
                hashValue = nextSlot(hashValue);
            }
            hashTable->at(hashValue) = buildTuple;
        }

        for (const auto &probeTuple: *probeSide) {
            const AttributeValue &probeValue = probeTuple.at(joinAttributeIndex);
            unsigned long hashValue = hash(probeValue);

            while (!hashTable->at(hashValue).empty() &&
                    hashTable->at(hashValue).at(joinAttributeIndex) != probeValue) {
                hashValue = nextSlot(hashValue);
            }

            const Tuple &buildTuple = hashTable->at(hashValue);

            if (buildTuple.at(joinAttributeIndex) == probeValue) {
                Tuple combined(buildTuple.size() + probeTuple.size());
                combined.insert(combined.begin(), probeTuple.begin(), probeTuple.end());
                combined.insert(combined.end(), buildTuple.begin(), buildTuple.end());
                result->push_back(combined);
            }
        }
    }

    return new Relation;
}

// TODO - add comments
void DBMSImplementationForMarks::merge(Relation *relation, const size_t begin, const size_t mid, const size_t end) {
    size_t leftSize = mid - begin + 1, rightSize = end - mid;
    size_t leftIndex = 0, rightIndex = 0, relationIndex;
    Relation leftSide(leftSize), rightSide(rightSize);

    for (; leftIndex < leftSize; leftIndex++) { leftSide[leftIndex] = relation->at(begin + leftIndex); }
    for (; rightIndex < rightSize; rightIndex++) { rightSide[rightIndex] = relation->at(mid + rightIndex + 1); }

    leftIndex = 0;
    rightIndex = 0;

    for (relationIndex = begin; leftIndex < leftSize && rightIndex < rightSize; relationIndex++) {
        if (lessThan(leftSide.at(leftIndex).at(joinAttributeIndex),
                     rightSide.at(rightIndex).at(joinAttributeIndex))) {
            relation->at(relationIndex) = leftSide.at(leftIndex++);
        } else {
            relation->at(relationIndex) = rightSide.at(rightIndex++);
        }
    }

    while (leftIndex < leftSize) { relation->at(relationIndex++) = leftSide.at(leftIndex++); }
    while (rightIndex < rightSize) { relation->at(relationIndex++) = rightSide.at(rightIndex++); }
}

// TODO - add comments
void DBMSImplementationForMarks::mergeSort(Relation *relation, const size_t begin, const size_t end) {
    if (begin < end) {
        size_t mid = (begin + end) / 2;
        mergeSort(relation, begin, mid);
        mergeSort(relation, mid + 1, end);
        merge(relation, begin, mid, end);
    }
}

// Sorts relation
void DBMSImplementationForMarks::sort(Relation *relation) {
    if (relation->size() < 2) { return; }
    mergeSort(relation, 0, relation->size() - 1);
}

// Implements sort-merge join algorithm
// Assumes both relations are sorted and contain unique values
const Relation *DBMSImplementationForMarks::sortMergeJoin(const Relation *leftSide,
                                                          const Relation *rightSide) {
    if (leftSide == nullptr || rightSide == nullptr) { return nullptr; }
    auto *result = new Relation; // buffer deleted by runQuery function
    size_t leftIndex = 0, rightIndex = 0;
    while (leftIndex < leftSide->size() && rightIndex < rightSide->size()) {
        const Tuple &leftTuple = leftSide->at(leftIndex);
        const Tuple &rightTuple = rightSide->at(rightIndex);

        auto leftValue = leftTuple.at(joinAttributeIndex);
        auto rightValue = rightTuple.at(joinAttributeIndex);


        if (lessThan(leftValue, rightValue)) leftIndex++;
        else if (lessThan(rightValue, leftValue)) rightIndex++;
        else {
            Tuple combined;
            combined.reserve(leftTuple.size() + rightTuple.size());
            combined.insert(combined.begin(), leftTuple.begin(), leftTuple.end());
            combined.insert(combined.end(), rightTuple.begin(), rightTuple.end());
            result->push_back(combined);
            leftIndex++;
        }
    }
    return result;
}

// TODO - review
// Selects tuples where sum of selected attribute values is greater than the threshold
const Relation *DBMSImplementationForMarks::select(const Relation *input, const int threshold) {
    if (input == nullptr) { return nullptr; }
    auto *result = new Relation;
    for (const auto &row: *input) {
        long largeOneValue = getLongValue(row[selectAttributeIndex]);
        long largeTwoValue = getLongValue(row[selectAttributeIndex + 3]);
        long smallValue = getLongValue(row[selectAttributeIndex + 6]);
        if ((largeOneValue + largeTwoValue + smallValue) > threshold) {
            result->push_back(row);
        }
    }
    return result;
}

// TODO - review
// Returns sum of product of the selected attribute values
long DBMSImplementationForMarks::sumOfProduct(const Relation *input) {
    if (input == nullptr) { return 0; }
    long sum = 0;
    for (const auto &row: *input) {
        long largeOneValue = getLongValue(row[sumAttributeIndex]);
        long largeTwoValue = getLongValue(row[sumAttributeIndex + 3]);
        long smallValue = getLongValue(row[sumAttributeIndex + 6]);

        sum += (largeOneValue * largeTwoValue * smallValue);
    }
    return sum;
}