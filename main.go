package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

const (
	// Максимальное разрешенное число url в запросе пользователя
	MaxUrlCount int = 20
	// Таймаут запроса одного url
	RequestUrlTimeout time.Duration = 1 * time.Second
	// Максимальное число одновременно обрабатываемых запросов
	MaxSimultaneousClients int = 100
	// Максимальное число одновременно обрабатываемых url в одном пользовательском запросе
	MaxSimultaneousUrlRequests int = 4
)

// Urls структура входящего запроса
type Urls struct {
	Urls []string `json:"urls"`
}

// UrlResult структура содержащая результат (Response) запроса (Url) и возникшую при этом ошибку (error)
type UrlResult struct {
	Url      string `json:"url"`
	Response []byte `json:"response"`
	error    error  // error служебное поле, не экспортируем
}

// ResultToUser структура конечно ответа пользователю
type ResultToUser struct {
	Error     string      `json:"error"`
	Responses []UrlResult `json:"responses"`
}

// RequestUrl запрашивает информацию по url с помощью Get-метода
// возвращает тело результата и ошибку.
// Если все ok, то error == nil
func RequestUrl(url string) ([]byte, error) {
	client := http.Client{
		Timeout: RequestUrlTimeout,
	}
	resp, err := client.Get(url)
	if err != nil {
		fmt.Println(err)
		return []byte{}, err
	}

	return ioutil.ReadAll(resp.Body)
}

// Запрашивает информацию по всем url в списке urls и записывает результат в канал out
func QueryUrls(urls []string, out chan<- UrlResult) {
	tasks := make(chan string, MaxSimultaneousUrlRequests)

	// эта горутина формирует список задач
	go func() {
		defer close(tasks)

		for _, url := range urls {
			tasks <- url
		}
	}()

	var wg sync.WaitGroup
	// в цикле на каждую задачу создается горутина,
	// которая запрашивает информацию по url и пишет в результирующий канал out
	for task := range tasks {
		wg.Add(1)
		go func(task string) {
			defer wg.Done()

			result, err := RequestUrl(task)
			out <- UrlResult{task, result, err}
		}(task)
	}

	wg.Wait()
}

// Handle обрабатывает непосредственно сам POST-запрос
func Handle(rw http.ResponseWriter, r *http.Request) {
	// проверяем HTTP-метод, сервер обрабатывает только POST
	if r.Method != http.MethodPost {
		http.Error(rw, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(rw, "Could not read body", http.StatusBadRequest)
		return
	}

	var request Urls
	if err = json.Unmarshal(body, &request); err != nil {
		http.Error(rw, "Incorrect json in request", http.StatusBadRequest)
		return
	}

	// Сервер не обрабатывает запросы, где число url больше MaxUrlCount
	if len(request.Urls) > MaxUrlCount {
		http.Error(rw, fmt.Sprintf("Maximum allowed urls in one request is %d", MaxUrlCount), http.StatusBadRequest)
		return
	}

	// Ставим оповещение на закрытие соединения клиентом
	notify := rw.(http.CloseNotifier).CloseNotify()
	go func() {
		// TODO: доработать обработчик, чтобы убивались все горутины и ничего не отправлялось
		<-notify
		fmt.Println("HTTP connection just closed.")
	}()

	results := ResultToUser{}
	out := make(chan UrlResult, len(request.Urls)) // канал результатов обработки urlов

	// опращиваем урлы
	go QueryUrls(request.Urls, out)

	// формируем итоговый ответ пользователю
	for i := 0; i < len(request.Urls); i++ {
		res := <-out
		if res.error != nil {
			// TODO: завершаем все остальные горутины
			results.Error = res.error.Error()
			fmt.Println("Error встретилась ", res.error.Error())
		}
		results.Responses = append(results.Responses, res)
	}

	// упаковываем и отправляем
	res, err := json.Marshal(results)
	if err != nil {
		// TODO: отправляем ошибку пользователю
		fmt.Println("Error on marshal")
		return
	}
	rw.Header().Set("Content-Type", "application/json")
	rw.Write(res)
}

// ClientCheck проверяет условие, что сервер не обслуживает больше 100 запросов одновременно
// конечно горутины будут висеть в ожидании, но зато не будут отклоняться запросы пользователей
func ClientCheck(h http.Handler) http.Handler {
	// limiter своего рода семафор для контроля числа одновременно обрабатывающихся запросов
	limiter := make(chan struct{}, MaxSimultaneousClients)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// пробуем взять значение из канала-семафора
		limiter <- struct{}{}
		defer func() { <-limiter }()

		// передаем запрос следующему хэндлу
		h.ServeHTTP(w, r)
	})
}

func main() {
	var (
		ListenAddr    string = ":8080"
		HandlePattern string = "/post"
	)

	http.Handle(HandlePattern, ClientCheck(http.HandlerFunc(Handle)))
	err := http.ListenAndServe(ListenAddr, http.DefaultServeMux)
	if err != nil {
		fmt.Println(err)
	}
}
