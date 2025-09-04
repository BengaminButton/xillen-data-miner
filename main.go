package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

var author = "t.me/Bengamin_Button t.me/XillenAdapter"

type DataPoint struct {
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
	Source    string                 `json:"source"`
	Category  string                 `json:"category"`
}

type DataMiner struct {
	dataPoints []DataPoint
	patterns   map[string]*regexp.Regexp
	stats      map[string]interface{}
}

type WebScraper struct {
	urls    []string
	headers map[string]string
	timeout time.Duration
}

type DataAnalyzer struct {
	data    []DataPoint
	results map[string]interface{}
}

func NewDataMiner() *DataMiner {
	return &DataMiner{
		dataPoints: make([]DataPoint, 0),
		patterns:   make(map[string]*regexp.Regexp),
		stats:      make(map[string]interface{}),
	}
}

func (dm *DataMiner) addDataPoint(data map[string]interface{}, source, category string) {
	point := DataPoint{
		Timestamp: time.Now(),
		Data:      data,
		Source:    source,
		Category:  category,
	}
	dm.dataPoints = append(dm.dataPoints, point)
}

func (dm *DataMiner) loadFromCSV(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	if len(records) < 2 {
		return fmt.Errorf("недостаточно данных в CSV файле")
	}

	headers := records[0]
	for i := 1; i < len(records); i++ {
		data := make(map[string]interface{})
		for j, value := range records[i] {
			if j < len(headers) {
				if num, err := strconv.ParseFloat(value, 64); err == nil {
					data[headers[j]] = num
				} else {
					data[headers[j]] = value
				}
			}
		}
		dm.addDataPoint(data, filename, "csv")
	}

	fmt.Printf("Загружено %d записей из CSV файла\n", len(records)-1)
	return nil
}

func (dm *DataMiner) saveToJSON(filename string) error {
	data, err := json.MarshalIndent(dm.dataPoints, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filename, data, 0644)
}

func (dm *DataMiner) loadFromJSON(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &dm.dataPoints)
}

func (dm *DataMiner) scrapeWebData(url string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	content := string(body)

	emailPattern := regexp.MustCompile(`[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}`)
	phonePattern := regexp.MustCompile(`\+?[1-9]\d{1,14}`)
	urlPattern := regexp.MustCompile(`https?://[^\s]+`)

	emails := emailPattern.FindAllString(content, -1)
	phones := phonePattern.FindAllString(content, -1)
	urls := urlPattern.FindAllString(content, -1)

	data := map[string]interface{}{
		"emails": emails,
		"phones": phones,
		"urls":   urls,
		"length": len(content),
	}

	dm.addDataPoint(data, url, "web")
	fmt.Printf("Извлечено данных с %s: %d email, %d телефонов, %d URL\n",
		url, len(emails), len(phones), len(urls))

	return nil
}

func (dm *DataMiner) analyzeData() map[string]interface{} {
	analysis := make(map[string]interface{})

	if len(dm.dataPoints) == 0 {
		return analysis
	}

	analysis["total_points"] = len(dm.dataPoints)

	sources := make(map[string]int)
	categories := make(map[string]int)

	for _, point := range dm.dataPoints {
		sources[point.Source]++
		categories[point.Category]++
	}

	analysis["sources"] = sources
	analysis["categories"] = categories

	timeRange := map[string]interface{}{
		"earliest": dm.dataPoints[0].Timestamp,
		"latest":   dm.dataPoints[len(dm.dataPoints)-1].Timestamp,
	}
	analysis["time_range"] = timeRange

	return analysis
}

func (dm *DataMiner) searchByPattern(field, pattern string) []DataPoint {
	var results []DataPoint
	regex, err := regexp.Compile("(?i)" + pattern)
	if err != nil {
		fmt.Printf("Ошибка компиляции регулярного выражения: %v\n", err)
		return results
	}

	for _, point := range dm.dataPoints {
		if value, exists := point.Data[field]; exists {
			if str, ok := value.(string); ok && regex.MatchString(str) {
				results = append(results, point)
			}
		}
	}

	return results
}

func (dm *DataMiner) filterByRange(field string, min, max float64) []DataPoint {
	var results []DataPoint

	for _, point := range dm.dataPoints {
		if value, exists := point.Data[field]; exists {
			if num, ok := value.(float64); ok && num >= min && num <= max {
				results = append(results, point)
			}
		}
	}

	return results
}

func (dm *DataMiner) getStatistics(field string) map[string]interface{} {
	stats := make(map[string]interface{})
	var values []float64

	for _, point := range dm.dataPoints {
		if value, exists := point.Data[field]; exists {
			if num, ok := value.(float64); ok {
				values = append(values, num)
			}
		}
	}

	if len(values) == 0 {
		return stats
	}

	sort.Float64s(values)

	sum := 0.0
	for _, v := range values {
		sum += v
	}

	stats["count"] = len(values)
	stats["sum"] = sum
	stats["mean"] = sum / float64(len(values))
	stats["min"] = values[0]
	stats["max"] = values[len(values)-1]
	stats["median"] = values[len(values)/2]

	variance := 0.0
	mean := stats["mean"].(float64)
	for _, v := range values {
		variance += math.Pow(v-mean, 2)
	}
	variance /= float64(len(values))
	stats["variance"] = variance
	stats["std_dev"] = math.Sqrt(variance)

	return stats
}

func (dm *DataMiner) exportToCSV(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if len(dm.dataPoints) == 0 {
		return nil
	}

	headers := []string{"timestamp", "source", "category"}
	fieldSet := make(map[string]bool)

	for _, point := range dm.dataPoints {
		for field := range point.Data {
			fieldSet[field] = true
		}
	}

	for field := range fieldSet {
		headers = append(headers, field)
	}

	writer.Write(headers)

	for _, point := range dm.dataPoints {
		record := []string{
			point.Timestamp.Format("2006-01-02 15:04:05"),
			point.Source,
			point.Category,
		}

		for _, field := range headers[3:] {
			if value, exists := point.Data[field]; exists {
				record = append(record, fmt.Sprintf("%v", value))
			} else {
				record = append(record, "")
			}
		}

		writer.Write(record)
	}

	return nil
}

func (dm *DataMiner) generateReport() string {
	analysis := dm.analyzeData()

	report := "=== ОТЧЁТ АНАЛИЗА ДАННЫХ ===\n"
	report += fmt.Sprintf("Всего точек данных: %v\n", analysis["total_points"])

	if sources, ok := analysis["sources"].(map[string]int); ok {
		report += "\nИсточники данных:\n"
		for source, count := range sources {
			report += fmt.Sprintf("  %s: %d\n", source, count)
		}
	}

	if categories, ok := analysis["categories"].(map[string]int); ok {
		report += "\nКатегории данных:\n"
		for category, count := range categories {
			report += fmt.Sprintf("  %s: %d\n", category, count)
		}
	}

	if timeRange, ok := analysis["time_range"].(map[string]interface{}); ok {
		report += "\nВременной диапазон:\n"
		report += fmt.Sprintf("  Начало: %v\n", timeRange["earliest"])
		report += fmt.Sprintf("  Конец: %v\n", timeRange["latest"])
	}

	return report
}

func (dm *DataMiner) cleanData() {
	fmt.Println("Очистка данных...")

	cleaned := make([]DataPoint, 0)
	duplicates := make(map[string]bool)

	for _, point := range dm.dataPoints {
		key := fmt.Sprintf("%v_%s_%s", point.Data, point.Source, point.Category)
		if !duplicates[key] {
			duplicates[key] = true
			cleaned = append(cleaned, point)
		}
	}

	removed := len(dm.dataPoints) - len(cleaned)
	dm.dataPoints = cleaned

	fmt.Printf("Удалено дубликатов: %d\n", removed)
	fmt.Printf("Осталось записей: %d\n", len(cleaned))
}

func (dm *DataMiner) aggregateData(field string, interval time.Duration) map[string]interface{} {
	aggregated := make(map[string]interface{})

	if len(dm.dataPoints) == 0 {
		return aggregated
	}

	groups := make(map[string][]DataPoint)

	for _, point := range dm.dataPoints {
		if value, exists := point.Data[field]; exists {
			intervalStart := point.Timestamp.Truncate(interval)
			key := intervalStart.Format("2006-01-02 15:04:05")
			groups[key] = append(groups[key], point)
		}
	}

	for interval, points := range groups {
		sum := 0.0
		count := 0

		for _, point := range points {
			if value, exists := point.Data[field]; exists {
				if num, ok := value.(float64); ok {
					sum += num
					count++
				}
			}
		}

		if count > 0 {
			aggregated[interval] = map[string]interface{}{
				"sum":   sum,
				"count": count,
				"avg":   sum / float64(count),
			}
		}
	}

	return aggregated
}

func main() {
	fmt.Println(author)
	fmt.Println("=== XILLEN DATA MINER ===")

	miner := NewDataMiner()
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Println("\n=== ГЛАВНОЕ МЕНЮ ===")
		fmt.Println("1. Загрузить данные из CSV")
		fmt.Println("2. Загрузить данные из JSON")
		fmt.Println("3. Сохранить данные в JSON")
		fmt.Println("4. Экспорт в CSV")
		fmt.Println("5. Веб-скрапинг")
		fmt.Println("6. Анализ данных")
		fmt.Println("7. Поиск по паттерну")
		fmt.Println("8. Фильтрация по диапазону")
		fmt.Println("9. Статистика по полю")
		fmt.Println("10. Очистка данных")
		fmt.Println("11. Агрегация данных")
		fmt.Println("12. Генерация отчёта")
		fmt.Println("0. Выход")

		fmt.Print("Выберите опцию: ")
		scanner.Scan()
		choice := strings.TrimSpace(scanner.Text())

		switch choice {
		case "1":
			loadFromCSV(miner, scanner)
		case "2":
			loadFromJSON(miner, scanner)
		case "3":
			saveToJSON(miner, scanner)
		case "4":
			exportToCSV(miner, scanner)
		case "5":
			webScraping(miner, scanner)
		case "6":
			analyzeData(miner)
		case "7":
			searchByPattern(miner, scanner)
		case "8":
			filterByRange(miner, scanner)
		case "9":
			getStatistics(miner, scanner)
		case "10":
			miner.cleanData()
		case "11":
			aggregateData(miner, scanner)
		case "12":
			fmt.Println(miner.generateReport())
		case "0":
			fmt.Println("До свидания!")
			return
		default:
			fmt.Println("Неверный выбор")
		}
	}
}

func loadFromCSV(miner *DataMiner, scanner *bufio.Scanner) {
	fmt.Print("Введите имя CSV файла: ")
	scanner.Scan()
	filename := scanner.Text()

	err := miner.loadFromCSV(filename)
	if err != nil {
		fmt.Printf("Ошибка загрузки: %v\n", err)
	}
}

func loadFromJSON(miner *DataMiner, scanner *bufio.Scanner) {
	fmt.Print("Введите имя JSON файла: ")
	scanner.Scan()
	filename := scanner.Text()

	err := miner.loadFromJSON(filename)
	if err != nil {
		fmt.Printf("Ошибка загрузки: %v\n", err)
	} else {
		fmt.Printf("Загружено %d записей\n", len(miner.dataPoints))
	}
}

func saveToJSON(miner *DataMiner, scanner *bufio.Scanner) {
	fmt.Print("Введите имя файла для сохранения: ")
	scanner.Scan()
	filename := scanner.Text()

	err := miner.saveToJSON(filename)
	if err != nil {
		fmt.Printf("Ошибка сохранения: %v\n", err)
	} else {
		fmt.Println("Данные сохранены")
	}
}

func exportToCSV(miner *DataMiner, scanner *bufio.Scanner) {
	fmt.Print("Введите имя CSV файла для экспорта: ")
	scanner.Scan()
	filename := scanner.Text()

	err := miner.exportToCSV(filename)
	if err != nil {
		fmt.Printf("Ошибка экспорта: %v\n", err)
	} else {
		fmt.Println("Экспорт завершён")
	}
}

func webScraping(miner *DataMiner, scanner *bufio.Scanner) {
	fmt.Print("Введите URL для скрапинга: ")
	scanner.Scan()
	url := scanner.Text()

	err := miner.scrapeWebData(url)
	if err != nil {
		fmt.Printf("Ошибка скрапинга: %v\n", err)
	}
}

func analyzeData(miner *DataMiner) {
	analysis := miner.analyzeData()
	fmt.Println("=== АНАЛИЗ ДАННЫХ ===")

	for key, value := range analysis {
		fmt.Printf("%s: %v\n", key, value)
	}
}

func searchByPattern(miner *DataMiner, scanner *bufio.Scanner) {
	fmt.Print("Введите поле для поиска: ")
	scanner.Scan()
	field := scanner.Text()

	fmt.Print("Введите паттерн поиска: ")
	scanner.Scan()
	pattern := scanner.Text()

	results := miner.searchByPattern(field, pattern)
	fmt.Printf("Найдено записей: %d\n", len(results))

	for i, result := range results {
		if i >= 10 {
			fmt.Println("... (показаны первые 10 результатов)")
			break
		}
		fmt.Printf("  %v\n", result.Data)
	}
}

func filterByRange(miner *DataMiner, scanner *bufio.Scanner) {
	fmt.Print("Введите поле для фильтрации: ")
	scanner.Scan()
	field := scanner.Text()

	fmt.Print("Введите минимальное значение: ")
	scanner.Scan()
	minStr := scanner.Text()
	min, err := strconv.ParseFloat(minStr, 64)
	if err != nil {
		fmt.Println("Неверное минимальное значение")
		return
	}

	fmt.Print("Введите максимальное значение: ")
	scanner.Scan()
	maxStr := scanner.Text()
	max, err := strconv.ParseFloat(maxStr, 64)
	if err != nil {
		fmt.Println("Неверное максимальное значение")
		return
	}

	results := miner.filterByRange(field, min, max)
	fmt.Printf("Найдено записей в диапазоне: %d\n", len(results))
}

func getStatistics(miner *DataMiner, scanner *bufio.Scanner) {
	fmt.Print("Введите поле для статистики: ")
	scanner.Scan()
	field := scanner.Text()

	stats := miner.getStatistics(field)
	fmt.Printf("=== СТАТИСТИКА ПО ПОЛЮ '%s' ===\n", field)

	for key, value := range stats {
		fmt.Printf("%s: %v\n", key, value)
	}
}

func aggregateData(miner *DataMiner, scanner *bufio.Scanner) {
	fmt.Print("Введите поле для агрегации: ")
	scanner.Scan()
	field := scanner.Text()

	fmt.Print("Введите интервал (1h, 1d, 1w): ")
	scanner.Scan()
	intervalStr := scanner.Text()

	var interval time.Duration
	switch intervalStr {
	case "1h":
		interval = time.Hour
	case "1d":
		interval = 24 * time.Hour
	case "1w":
		interval = 7 * 24 * time.Hour
	default:
		fmt.Println("Неверный интервал")
		return
	}

	aggregated := miner.aggregateData(field, interval)
	fmt.Printf("=== АГРЕГАЦИЯ ПО ПОЛЮ '%s' ===\n", field)

	for interval, data := range aggregated {
		fmt.Printf("%s: %v\n", interval, data)
	}
}
