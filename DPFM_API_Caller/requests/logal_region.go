package requests

type LocalRegion struct {
	LocalRegion			string	`json:"LocalRegion"`
	Country				string	`json:"Country"`
	CreationDate		string	`json:"CreationDate"`
	LastChangeDate		string	`json:"LastChangeDate"`
	IsMarkedForDeletion	*bool	`json:"IsMarkedForDeletion"`
}
