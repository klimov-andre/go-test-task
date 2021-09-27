package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
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

// UrlResult структура содержащая результат (Response) запроса Url (Url) и возникшую при этом ошибку (error)
type UrlResult struct {
	Url      string `json:"url"`
	Response []byte `json:"response"`
	error    error  // error служебное поле, не экспортируем
}

// ResultToUser структура итогового ответа пользователю
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
		return []byte{}, err
	}

	return ioutil.ReadAll(resp.Body)
}

// QueryUrls асинхронно запрашивает информацию по всем url в списке (urls) и записывает результат в канал (out)
// parentWg - WaitGroup вызывающего метода
// urls список url
// workersCount кол-во одновременно запрашивающих горутин
// out канал для записи результатов
// quit канал для опроса экстренного выхода
func QueryUrls(parentWg *sync.WaitGroup, urls []string, workersCount int, out chan<- UrlResult, quit chan struct{}) {
	defer parentWg.Done()
	tasks := make(chan string, len(urls)) // список urlов-задач

	var wg sync.WaitGroup
	// создаем рабочие горутины, которые будут посылать запросы
	for i := 0; i < workersCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				select {
				case task, ok := <-tasks:
					// канал закрыт, значит уже нет заданий и можно завершаться
					if !ok {
						return
					}
					result, err := RequestUrl(task)
					out <- UrlResult{task, result, err}

				case <-quit:
					// прекращаем работу
					return
				}
			}
		}()
	}

	//список задач спокойно формируем синхронно
	for _, url := range urls {
		tasks <- url
	}
	// все задачи сформированы, можно закрыть канал
	close(tasks)
	// ждем завершения работающих горутин
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

	results := ResultToUser{}
	pipeline := make(chan UrlResult, len(request.Urls)) // канал результатов обработки urlов
	quit := make(chan struct{})                         // канал обработки закрытия соединения клиентом

	// количество одновременно запрашивающих горутин не больше MaxSimultaneousUrlRequests
	workersCount := MaxSimultaneousUrlRequests
	if len(request.Urls) < MaxSimultaneousUrlRequests {
		workersCount = len(request.Urls)
	}

	// опращиваем урлы
	var wait sync.WaitGroup
	wait.Add(1)
	go QueryUrls(&wait, request.Urls, workersCount, pipeline, quit)

	needToSend := true // по умолчанию результаты отослать надо, но если сервер закрыл соединение - то нет

	// Ставим оповещение на закрытие соединения клиентом
	connectionClose := rw.(http.CloseNotifier).CloseNotify()

	// формируем итоговый ответ пользователю
Loop:
	for i := 0; i < len(request.Urls); i++ {
		select {
		case <-connectionClose:
			// оповещаем рабочие горутины о необходимости завершения
			close(quit)
			// в этом случае отправлять пользователю ничего не надо, т.к. уже некуда
			needToSend = false
			break Loop

		case res := <-pipeline:
			// при ошибке в обработке хоть одного url завершаем работу
			if res.error != nil {
				// завершаем все остальные горутины
				close(quit)
				// пишем ошибку в результирующую структуру
				results.Error = res.error.Error()
				// результаты запросов из ответа убираем
				results.Responses = nil
				break Loop
			} else {
				results.Responses = append(results.Responses, res)
			}

		}
	}

	// Ожидаем завершения всех работающих горутин
	wait.Wait()

	if needToSend {
		// упаковываем и отправляем
		res, err := json.Marshal(results)
		if err != nil {
			log.Println("Error on marshal ", err.Error())
			http.Error(rw, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		rw.Header().Set("Content-Type", "application/json")
		rw.Write(res)
	}

}

// HandleConnection проверяет условие, что сервер не обслуживает больше 100 запросов одновременно
// конечно горутины будут висеть в ожидании, но зато не будут отклоняться запросы пользователей
// shutdown служит индикатором того, что придется закрыть все соединения
// h следующий хэндлер
func HandleConnection(shutdown chan struct{}, h http.Handler) http.Handler {
	// limiter своего рода семафор для контроля числа одновременно обрабатывающихся запросов
	limiter := make(chan struct{}, MaxSimultaneousClients)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-shutdown: // нотификация от системы на завершение
			// чтобы пользователь не волновался, скинем ему ошибку
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return

		case limiter <- struct{}{}: // пробуем добавить значение в канал-семафор
			defer func() { <-limiter }()
			// передаем запрос следующему хэндлу
			h.ServeHTTP(w, r)
		}
	})
}

func main() {
	var (
		ListenAddr    string = ":8080"
		HandlePattern string = "/post"
	)

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	// т.к. shutdown не закрывается, поэтому не очень удобно осуществлять закрытие висящих в ожидании соединений
	// quit будет закрываться при появлении сигнала из системы
	quit := make(chan struct{})

	// создаем сервер
	mux := http.NewServeMux()
	mux.Handle(HandlePattern, HandleConnection(quit, http.HandlerFunc(Handle)))
	server := &http.Server{Addr: ListenAddr, Handler: mux}

	// запускаем сервер
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Println("ListenAndServe: ", err)
		}
	}()
	log.Println("Server started")

	// блочимся до того момента, пока пользователь или система не прервет исполнение
	<-shutdown
	log.Println("Interruption from OS")

	// исполнение прервано, оповещаем об этом ждущие горутины, путем закрытия канала quit
	close(quit)
	// выключаем сервер
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := server.Shutdown(ctx); err != nil {
		log.Println(err)
	}
	cancel()

	log.Println("Server stopped")
}
