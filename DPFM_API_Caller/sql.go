package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-local-region-reads-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-local-region-reads-rmq-kube/DPFM_API_Output_Formatter"
	"fmt"
	"strings"
	"sync"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) readSqlProcess(
	ctx context.Context,
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	errs *[]error,
	log *logger.Logger,
) interface{} {
	var localRegions *[]dpfm_api_output_formatter.LocalRegion
	var text *[]dpfm_api_output_formatter.Text
	for _, fn := range accepter {
		switch fn {
		case "LocalRegion":
			func() {
				localRegions = c.LocalRegion(mtx, input, output, errs, log)
			}()
		case "LocalRegions":
			func() {
				localRegions = c.LocalRegions(mtx, input, output, errs, log)
			}()
		case "Text":
			func() {
				text = c.Text(mtx, input, output, errs, log)
			}()
		case "Texts":
			func() {
				text = c.Texts(mtx, input, output, errs, log)
			}()
		default:
		}
	}

	data := &dpfm_api_output_formatter.Message{
		LocalRegion: localRegions,
		Text:      text,
	}

	return data
}

func (c *DPFMAPICaller) LocalRegion(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.LocalRegion {
	where := fmt.Sprintf("WHERE LocalRegion = '%s'", input.LocalRegion.LocalRegion)

	if input.LocalRegion.Country != nil {
		where = fmt.Sprintf("%s\nAND Country = '%s'", where, *input.LocalRegion.Country)
	}
	if input.LocalRegion.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s\nAND IsMarkedForDeletion = %v", where, *input.LocalRegion.IsMarkedForDeletion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_local_region_local_region_data
		` + where + ` ORDER BY IsMarkedForDeletion ASC, Country ASC, LocalRegion ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToLocalRegion(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) LocalRegions(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.LocalRegion {
	where := fmt.Sprintf("WHERE LocalRegion = '%s'", input.LocalRegion.LocalRegion)

	if input.LocalRegion.Country != nil {
		where = fmt.Sprintf("%s\nAND Country = '%s'", where, *input.LocalRegion.Country)
	}
	if input.LocalRegion.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s\nAND IsMarkedForDeletion = %v", where, *input.LocalRegion.IsMarkedForDeletion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_local_region_local_region_data
		` + where + ` ORDER BY IsMarkedForDeletion ASC, Country ASC, LocalRegion ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToLocalRegion(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) Text(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Text {
	var args []interface{}
	localRegion := input.LocalRegion.LocalRegion
	country := input.LocalRegion.Country
	text := input.LocalRegion.Text

	cnt := 0
	for _, v := range text {
		args = append(args, localRegion, country, v.Language)
		cnt++
	}

	repeat := strings.Repeat("(?,?),", cnt-1) + "(?,?)"
	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_local_region_text_data
		WHERE (LocalRegion, Country, Language) IN ( `+repeat+` );`, args...,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToText(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) Texts(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Text {
	var args []interface{}
	text := input.LocalRegion.Text

	cnt := 0
	for _, v := range text {
		args = append(args, v.Language)
		cnt++
	}

	repeat := strings.Repeat("(?),", cnt-1) + "(?)"
	rows, err := c.db.Query(
		`SELECT * 
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_local_region_text_data
		WHERE Language IN ( `+repeat+` );`, args...,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	//
	data, err := dpfm_api_output_formatter.ConvertToText(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}
