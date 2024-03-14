package dpfm_api_output_formatter

import (
	"data-platform-api-local-region-reads-rmq-kube/DPFM_API_Caller/requests"
	"database/sql"
	"fmt"
)

func ConvertToLocalRegion(rows *sql.Rows) (*[]LocalRegion, error) {
	defer rows.Close()
	localRegion := make([]LocalRegion, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.LocalRegion{}

		err := rows.Scan(
			&pm.LocalRegion,
			&pm.Country,
			&pm.CreationDate,
			&pm.LastChangeDate,
			&pm.IsMarkedForDeletion,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &localRegion, nil
		}

		data := pm
		localRegion = append(localRegion, LocalRegion{
			LocalRegion:			data.LocalRegion,
			Country:				data.Country,
			CreationDate:			data.CreationDate,
			LastChangeDate:			data.LastChangeDate,
			IsMarkedForDeletion:	data.IsMarkedForDeletion,
		})
	}

	return &localRegion, nil
}

func ConvertToText(rows *sql.Rows) (*[]Text, error) {
	defer rows.Close()
	text := make([]Text, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.Text{}

		err := rows.Scan(
			&pm.LocalRegion,
			&pm.Country,
			&pm.Language,
			&pm.LocalRegionName,
			&pm.CreationDate,
			&pm.LastChangeDate,
			&pm.IsMarkedForDeletion,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &text, err
		}

		data := pm
		text = append(text, Text{
			LocalRegion:     		data.LocalRegion,
			Country:				data.Country,
			Language:          		data.Language,
			LocalRegionName:		data.LocalRegionName,
			CreationDate:			data.CreationDate,
			LastChangeDate:			data.LastChangeDate,
			IsMarkedForDeletion:	data.IsMarkedForDeletion,
		})
	}

	return &text, nil
}
