from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_sort_and_percentage)
        ]

    def mapper_get_ratings(self, _, line):
        try:
            userID, movieID, rating, timestamp = line.split('\t')
            yield rating, 1
        except ValueError:
            pass  # skip malformed lines

    def reducer_count_ratings(self, rating, counts):
        total = sum(counts)
        yield None, (rating, total)  # group all for final reducer

    def reducer_sort_and_percentage(self, _, rating_count_pairs):
        data = list(rating_count_pairs)
        total_ratings = sum(count for _, count in data)
        
        # Sort by frequency, descending
        sorted_data = sorted(data, key=lambda x: x[1], reverse=True)

        for rating, count in sorted_data:
            percentage = (count / total_ratings) * 100
            yield rating, {
                'count': count,
                'percentage': round(percentage, 2)
            }

if __name__ == '__main__':
    RatingsBreakdown.run()
