#ifndef ADVANCEDDB2022COURSEWORK1_H
#define ADVANCEDDB2022COURSEWORK1_H

#include <array>
#include <cstdlib>
#include <tuple>
#include <variant>
#include <vector>
// YOU MAY NOT ADD ANY OTHER INCLUDES!!!
using AttributeValue = std::variant<long, double, char const *>;
using Tuple = std::vector<AttributeValue>;
using Relation = std::vector<Tuple>;

/**
 * 0 is long, 1 is double, 2 is a c-string
 */
inline size_t getAttributeValueType(AttributeValue const &value) { return value.index(); }

inline long getLongValue(AttributeValue const &value) { return std::get<long>(value); }

inline double getdoubleValue(AttributeValue const &value) { return std::get<double>(value); }

inline char const *getStringValue(AttributeValue const &value) {
    return std::get<char const *>(value);
}

inline size_t getNumberOfValuesInTuple(Tuple const &t) { return t.size(); }

inline size_t getNumberOfTuplesInRelation(Relation const &t) { return t.size(); }

/**
 * DBMS shall implement the following query in the query function:
 *
 * select sumOfProduct(large1.c * large2.c * small.c) from large1, large2, small where
 * large1.a = large2.a and large2.a = small.a and large1.b + large2.b + small.b
 * > 9;
 */
class DBMSImplementationForMarks {
    // **POINTERS TO RELATIONS**
    // Lifetimes are guaranteed to be longer than DBMS implementation so no need to garbage collect

    Relation *large1, *large2, *small;

    // **QUERY PARAMETERS**

    static constexpr size_t joinAttributeIndex = 0; // a
    static constexpr size_t selectAttributeIndex = 1; // b
    static constexpr size_t sumAttributeIndex = 2; // c

    // **MAIN QUERY FUNCTIONS**

    // General purpose comparison function for weakly typed attribute values
    // Returns a pair where the second boolean value determines if the comparison was valid and safe
    // The first value returns an int which is left - right for numerical types and strcmp for c-strings
    static std::pair<int, bool> comp(const AttributeValue &left, const AttributeValue &right);

    // Checks less than constraint on two weakly typed attribute values
    // Returns a pair of bools where the first is the result of the comparison and the second is its validity
    static bool lessThan(const AttributeValue &left, const AttributeValue &right);

    // Checks equality constraint on two weakly typed attribute values
    // Returns a pair of bools where the first is the result of the comparison and the second is its validity
    static bool equals(const AttributeValue &left, const AttributeValue &right);

    // Implements hash join algorithm
    // Smaller relation should be used as the buildSide
    static const Relation *hashJoin(const Relation *probeSide, const Relation *buildSide);

    // TODO - add comments
    static void merge(Relation *relation, size_t begin, size_t mid, size_t end);

    // TODO - add comments
    static void mergeSort(Relation *relation, size_t begin, size_t end);

    // Returns new sorted relation
    static void sort(Relation *relation);

    // Implements sort-merge join algorithm
    // Assumes both relations are sorted and contain unique values
    static const Relation *sortMergeJoin(const Relation *leftSide, const Relation *rightSide);

    // Selects tuples where sum of selected attribute values is greater than the threshold
    static const Relation *select(const Relation *input, long threshold);

    // Returns sum of product of the selected attribute values
    static long sumOfProduct(const Relation *input);

public:
    void loadData(Relation const *l1,
                  Relation const *l2,
                  Relation const *s) {

        // store pointers to relations
        large1 = new Relation(*l1);
        large2 = new Relation(*l2);
        small = new Relation(*s);

        // sort tables
        sort(large1);
        sort(large2);

        // TODO - build hash table for small
    }

    long runQuery(long threshold = 9) {
        const Relation *buffer1 = sortMergeJoin(large1, large2);
        const Relation *buffer2 = hashJoin(buffer1, small);
        const Relation *buffer3 = select(buffer2, threshold);
        long result = sumOfProduct(buffer3);

        // clean up buffers on heap
        delete buffer1;
        delete buffer2;
        delete buffer3;

        // delete saved tables
        delete large1;
        delete large2;
        delete small;

        return result;
    }
};

class DBMSImplementationForCompetition : public DBMSImplementationForMarks {
public:
    static constexpr char const *teamName = nullptr;
};

#endif /* ADVANCEDDB2022COURSEWORK1_H */
