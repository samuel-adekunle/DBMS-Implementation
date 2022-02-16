#include "AdvancedDB2022Coursework1.hpp"

// **HELPER FUNCTIONS FOR COMPARISON OF WEAKLY TYPED VALUES**

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

// Checks equality constraint on two weakly typed attribute values
// Returns a pair of bools where the first is the result of the comparison and the second is its validity
bool DBMSImplementationForMarks::equals(const AttributeValue &left, const AttributeValue &right) {
    auto[value, valid] = comp(left, right);
    return (value == 0) && valid;
}

// **SORTING FUNCTIONS

// Mergesort helper function to merge two sorted sections of the relation
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

// Mergesort algorithm implementation
void DBMSImplementationForMarks::mergeSort(Relation *relation, const size_t begin, const size_t end) {
    if (begin < end) {
        size_t mid = (begin + end) / 2;
        mergeSort(relation, begin, mid);
        mergeSort(relation, mid + 1, end);
        merge(relation, begin, mid, end);
    }
}

// Sorts a relation
// Overwrites the input
void DBMSImplementationForMarks::sort(Relation *relation) {
    if (relation == nullptr || relation->size() < 2) { return; }
    mergeSort(relation, 0, relation->size() - 1);
}

// **HASHING FUNCTIONS

// Checks if a given number is prime
bool DBMSImplementationForMarks::isPrime(const size_t n) {
    // Corner cases
    if (n <= 1) return false;
    if (n <= 3) return true;

    // This is checked so that we can skip
    // middle five numbers in below loop
    if (n % 2 == 0 || n % 3 == 0) return false;

    for (int i = 5; i * i <= n; i = i + 6)
        if (n % i == 0 || n % (i + 2) == 0)
            return false;

    return true;
}

// Function to return the smallest prime number greater than N
size_t DBMSImplementationForMarks::nextPrime(const size_t N) {
    // Base case
    if (N <= 1)
        return 2;

    size_t prime = N;
    bool found = false;

    // Loop continuously until isPrime returns
    // true for a number greater than n
    while (!found) {
        prime++;

        if (isPrime(prime))
            found = true;
    }

    return prime;
}

// Implement Modulo Division Hashing on Attribute Values
size_t DBMSImplementationForMarks::hash(const AttributeValue &value, const size_t key) {
    switch (getAttributeValueType(value)) {
        case 0: {
            return getLongValue(value) % key;
        }
        case 1: {
            return size_t(getdoubleValue(value)) % key;
        }
        case 2: {
            auto stringVal = getStringValue(value);
            if (stringVal == nullptr) return 0;
            return std::hash<const char *>{}(getStringValue(value)) % key;
        }
        default: {
            return 0;
        }
    }
}

// Implements Linear Probing for hash table
size_t DBMSImplementationForMarks::nextSlot(const size_t slot, const size_t hashTableSize) {
    return (slot + 1) % hashTableSize;
}

// **MAIN QUERY FUNCTIONS**

// Builds a hash-index for the given relation
std::pair<const Relation *, size_t> DBMSImplementationForMarks::hashBuild(const Relation *buildSide) {
    if (buildSide == nullptr) { return {nullptr, 0}; }
    const size_t hashTableSize = buildSide->size() * 2;
    const size_t key = nextPrime(size_t(buildSide->size() * 0.75));

    auto *table = new Relation(hashTableSize);

    for (const auto &buildTuple: *buildSide) {
        const AttributeValue &buildValue = buildTuple.at(joinAttributeIndex);
        if (buildValue.index() == 2 && getStringValue(buildValue) == nullptr)
            continue;
        unsigned long hashValue = hash(buildValue, key);
        while (!table->at(hashValue).empty()) {
            hashValue = nextSlot(hashValue, hashTableSize);
        }
        table->at(hashValue) = buildTuple;
    }

    return {table, key};
}

// Implements hash join algorithm by probing the given hash table
const Relation *DBMSImplementationForMarks::hashProbe(const Relation *probeSide, const Relation *hashTable,
                                                      const size_t hashKey) {
    auto *result = new Relation;
    const size_t hashTableSize = hashTable->size();
    for (const auto &probeTuple: *probeSide) {
        const AttributeValue &probeValue = probeTuple.at(joinAttributeIndex);
        if (probeValue.index() == 2 && getStringValue(probeValue) == nullptr) { continue; }

        unsigned long hashValue = hash(probeValue, hashKey);

        while (!hashTable->at(hashValue).empty()) {
            while (!hashTable->at(hashValue).empty() &&
                   !equals(hashTable->at(hashValue).at(joinAttributeIndex), probeValue)) {
                hashValue = nextSlot(hashValue, hashTableSize);
            }
            const Tuple &buildTuple = hashTable->at(hashValue);
            if (buildTuple.empty())
                continue;
            if (equals(buildTuple.at(joinAttributeIndex), probeValue)) {
                Tuple combined;
                combined.reserve(buildTuple.size() + probeTuple.size());
                combined.insert(combined.begin(), probeTuple.begin(), probeTuple.end());
                combined.insert(combined.end(), buildTuple.begin(), buildTuple.end());
                result->push_back(combined);
            }
            hashValue = nextSlot(hashValue, hashTableSize);
        }
    }
    return result;
}

// Implements sort-merge join algorithm
// Assumes both relations are sorted and contain unique values
const Relation *DBMSImplementationForMarks::sortMergeJoin(const Relation *leftSide,
                                                          const Relation *rightSide) {
    if (leftSide == nullptr || rightSide == nullptr) { return nullptr; }
    auto *result = new Relation;
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
            rightIndex++;
        }
    }
    return result;
}

// TODO - review
// Selects tuples where sum of selected attribute values is greater than the threshold
const Relation *DBMSImplementationForMarks::select(const Relation *input, const long threshold = 9) {
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