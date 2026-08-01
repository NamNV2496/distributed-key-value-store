package main

// import (
// 	"context"
// 	"fmt"
// 	"log"
// 	"time"

// 	"github.com/redis/go-redis/v9"
// )

// var (
// 	ctx = context.Background()
// 	rdb *redis.Client
// )

// type Period string

// const (
// 	Day   Period = "day"
// 	Week  Period = "week"
// 	Month Period = "month"
// 	Year  Period = "year"
// )

// type UserScore struct {
// 	UserID string
// 	Score  float64
// 	Rank   int64
// }

// func main() {

// 	rdb = redis.NewClient(&redis.Options{
// 		Addr: "localhost:6379",
// 	})

// 	if err := rdb.Ping(ctx).Err(); err != nil {
// 		log.Fatal(err)
// 	}

// 	// Demo data
// 	AddScore("alice", 120)
// 	AddScore("bob", 80)
// 	AddScore("carol", 200)
// 	AddScore("alice", 50)
// 	AddScore("david", 160)
// 	AddScore("eva", 90)

// 	fmt.Println("===== DAILY =====")
// 	PrintLeaderboard(Day)

// 	fmt.Println()

// 	fmt.Println("===== WEEKLY =====")
// 	PrintLeaderboard(Week)

// 	fmt.Println()

// 	fmt.Println("===== MONTHLY =====")
// 	PrintLeaderboard(Month)

// 	fmt.Println()

// 	rank, score, _ := GetUserRank(Day, "alice")
// 	fmt.Printf("Alice Rank=%d Score=%.0f\n", rank, score)
// }

// func AddScore(userID string, score float64) error {

// 	now := time.Now()

// 	keys := []string{
// 		BuildKey(Day, now),
// 		BuildKey(Week, now),
// 		BuildKey(Month, now),
// 	}
// 	for _, key := range keys {
// 		if err := rdb.ZIncrBy(ctx, key, score, userID).Err(); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

// func PrintLeaderboard(period Period) {

// 	key := BuildKey(period, time.Now())

// 	results, err := rdb.ZRevRangeWithScores(ctx, key, 0, 9).Result()
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	for i, item := range results {
// 		fmt.Printf(
// 			"#%d %-10s %.0f\n",
// 			i+1,
// 			item.Member.(string),
// 			item.Score,
// 		)
// 	}
// }

// func GetUserRank(period Period, userID string) (int64, float64, error) {

// 	key := BuildKey(period, time.Now())

// 	rank, err := rdb.ZRevRank(ctx, key, userID).Result()
// 	if err != nil {
// 		return 0, 0, err
// 	}

// 	score, err := rdb.ZScore(ctx, key, userID).Result()
// 	if err != nil {
// 		return 0, 0, err
// 	}

// 	return rank + 1, score, nil
// }

// func BuildKey(period Period, t time.Time) string {
// 	switch period {
// 	case Day:
// 		return fmt.Sprintf("lb:day:%s",
// 			t.Format("20060102"))
// 	case Week:
// 		year, week := t.ISOWeek()

// 		return fmt.Sprintf(
// 			"lb:week:%d%02d",
// 			year,
// 			week,
// 		)
// 	case Month:
// 		return fmt.Sprintf(
// 			"lb:month:%s",
// 			t.Format("200601"),
// 		)
// 	case Year:
// 		return fmt.Sprintf(
// 			"lb:year:%s",
// 			t.Format("2006"),
// 		)
// 	}

// 	panic("invalid period")
// }
