from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsAnalysis(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_extract_data,
                   reducer=self.reducer_aggregate_ratings),
            MRStep(mapper=self.mapper_filter_top_movies,
                   reducer=self.reducer_output_results)
        ]

    def mapper_extract_data(self, _, line):
        try:
            userID, movieID, rating, timestamp = line.strip().split('\t')
            rating = float(rating)

            # Emit movieID with rating and count
            yield movieID, (rating, 1)

            # Emit a special key to count total rows
            yield '__total__', 1

        except ValueError:
            pass  # skip malformed lines

    def reducer_aggregate_ratings(self, key, values):
        if key == '__total__':
            # Count total number of ratings
            yield '__total__', sum(values)
        else:
            total_rating = 0
            total_count = 0
            for rating, count in values:
                total_rating += rating
                total_count += count
            average_rating = total_rating / total_count
            # Yield to next step for filtering and sorting
            yield 'movie', (average_rating, total_count, key)  # key = movieID

    def mapper_filter_top_movies(self, key, value):
        if key == '__total__':
            yield '__total__', value
        elif key == 'movie':
            average_rating, count, movieID = value
            if count >= 20:
                # Only consider movies with at least 20 ratings
                yield 'top_movies', (average_rating, count, movieID)

    def reducer_output_results(self, key, values):
        if key == '__total__':
            for total in values:
                yield 'Total Ratings Processed', total

        elif key == 'top_movies':
            # Get top 10 movies by average rating
            top_10 = sorted(values, reverse=True)[:10]
            for avg, count, movieID in top_10:
                yield f"MovieID {movieID}", {
                    'average_rating': round(avg, 2),
                    'rating_count': count
                }

if __name__ == '__main__':
    RatingsAnalysis.run()
