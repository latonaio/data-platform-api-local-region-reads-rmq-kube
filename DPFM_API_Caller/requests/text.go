package requests

type Text struct {
	LocalRegion     	string  `json:"LocalRegion"`
	Country				string	`json:"Country"`
	Language          	string  `json:"Language"`
	LocalRegionName		string  `json:"LocalRegionName"`
	CreationDate		string	`json:"CreationDate"`
	LastChangeDate		string	`json:"LastChangeDate"`
	IsMarkedForDeletion	*bool	`json:"IsMarkedForDeletion"`
}
