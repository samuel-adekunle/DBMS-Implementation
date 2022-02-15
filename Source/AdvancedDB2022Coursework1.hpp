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

    const Relation *large1, *large2, *small;

    // **QUERY PARAMETERS**

    const size_t sortAttributeIndex = 0; // a
    const size_t joinAttributeIndex = 0; // a
    const size_t selectAttributeIndex = 1; // b
    const size_t sumAttributeIndex = 2; // c

    // **HELPER FUNCTIONS TO CHECK CONSTRAINTS ON WEAKLY TYPED ATTRIBUTE VALUES**

    // Checks equality constraint on two weakly typed attribute values
    inline bool equals(const AttributeValue &left, const AttributeValue &right);

    // Checks less than constraint on two weakly typed attribute values
    inline bool lessThan(const AttributeValue &left, const AttributeValue &right);

    // **MAIN QUERY FUNCTIONS**

    // Implements hash join algorithm
    // Smaller relation should be used as the buildSide
    const Relation *hashJoin(const Relation *probeSide, const Relation *buildSide, size_t attributeIndex);

    // Returns a sorted relation
    const Relation *sortRelation(const Relation *relation, size_t attributeIndex);

    // Implements sort-merge join algorithm
    const Relation *sortMergeJoin(const Relation *leftSide, const Relation *rightSide, size_t attributeIndex);

    // Selects tuples where sum of b attribute values is greater than the threshold
    const Relation *select(const Relation *input, int threshold, size_t attributeIndex);

    // Returns sum of product of c attribute values
    long sumOfProduct(const Relation *input, size_t attributeIndex);

public:
    void loadData(Relation const *l1,
                  Relation const *l2,
                  Relation const *s) {

        // save pointers to tables
        large1 = sortRelation(l1, sortAttributeIndex);
        large2 = l2;
        small = s;
    }

    long runQuery(long threshold = 9) {
        const Relation *buffer1 = hashJoin(large2, small, joinAttributeIndex);
        const Relation *buffer2 = sortRelation(buffer1, sortAttributeIndex);
        const Relation *buffer3 = sortMergeJoin(large1, buffer2, joinAttributeIndex);
        const Relation *buffer4 = select(buffer2, threshold, selectAttributeIndex);
        long result = sumOfProduct(buffer4, sumAttributeIndex);

        // clean up buffers on heap
        delete buffer1;
        delete buffer2;
        delete buffer3;
        delete buffer4;

        return result;
    }
};

class DBMSImplementationForCompetition : public DBMSImplementationForMarks {
public:
    static constexpr char const *teamName = nullptr;
};

#endif /* ADVANCEDDB2022COURSEWORK1_H */
