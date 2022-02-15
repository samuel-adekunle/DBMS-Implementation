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

    const Relation *sortedLarge1, *sortedLarge2;
    // **QUERY PARAMETERS**

    static constexpr size_t joinAttributeIndex = 0; // a
    static constexpr size_t selectAttributeIndex = 1; // b
    static constexpr size_t sumAttributeIndex = 2; // c

    // **MAIN QUERY FUNCTIONS**

    // Implements hash join algorithm
    // Smaller relation should be used as the buildSide
    static const Relation *hashJoin(const Relation *probeSide, const Relation *buildSide);

    static const Relation *merge(const Relation *leftSide, const Relation *rightSide);

    static const Relation *mergeSort(const Relation::const_iterator &begin, const Relation::const_iterator &end);

    // Returns new sorted relation
    static const Relation *sorted(const Relation *relation);

    // Implements sort-merge join algorithm
    // Assumes both relations are sorted and contain unique values
    static const Relation *sortMergeJoin(const Relation *leftSide, const Relation *rightSide);

    // Selects tuples where sum of selected attribute values is greater than the threshold
    static const Relation *select(const Relation *input, int threshold);

    // Returns sum of product of the selected attribute values
    static long sumOfProduct(const Relation *input);

public:
    void loadData(Relation const *l1,
                  Relation const *l2,
                  Relation const *s) {

        // store pointers to relations
        large1 = l1;
        large2 = l2;
        small = s;

        // pre-sort large 1 and large 2
        sortedLarge1 = sorted(large1);
        sortedLarge2 = sorted(large2);
    }

    long runQuery(long threshold = 9) {
        const Relation *buffer1 = sortMergeJoin(sortedLarge1, sortedLarge2);

        const Relation *buffer2;
        if (buffer1->size() < small->size()) {
            buffer2 = hashJoin(small, buffer1);
        } else {
            buffer2 = hashJoin(buffer1, small);
        }

        const Relation *buffer3 = select(buffer2, threshold);

        long result = sumOfProduct(buffer3);

        // clean up buffers on heap
        delete buffer1;
        delete buffer2;
        delete buffer3;

        // delete sorted tables
        delete sortedLarge1;
        delete sortedLarge2;

        return result;
    }
};

class DBMSImplementationForCompetition : public DBMSImplementationForMarks {
public:
    static constexpr char const *teamName = nullptr;
};

#endif /* ADVANCEDDB2022COURSEWORK1_H */
