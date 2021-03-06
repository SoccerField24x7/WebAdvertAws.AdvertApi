using AdvertApi.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AutoMapper;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;

namespace AdvertApi.Services
{
    public class DynamoDBAdvertStorage : IAdvertStorageService
    {
        private readonly IMapper _mapper;

        public DynamoDBAdvertStorage(IMapper mapper)
        {
            _mapper = mapper;
        }

        public IMapper Mapper { get; }

        public async Task<string> Add(AdvertModel model)
        {
            // TODO: Add validation
            var dbModel = _mapper.Map<AdvertDbModel>(model);
            using (var client = new AmazonDynamoDBClient()) // because credentials in OS and we have appsettings, no params needed
            {
                dbModel.Id = Guid.NewGuid().ToString();
                dbModel.CreationDateTime = DateTime.UtcNow;
                dbModel.Status = AdvertStatus.Pending;

                using (var context = new DynamoDBContext(client))
                {
                    await context.SaveAsync(dbModel);
                }
            }

            return dbModel.Id;
        }

        public async Task<bool> CheckHealthAsync()
        {
            using (var client = new AmazonDynamoDBClient())
            {
                var tableData = await client.DescribeTableAsync("Adverts");
                return string.Compare(tableData.Table.TableStatus, "active", true) == 0;
            }
        }

        public async Task Confirm(ConfirmAdvertModel model)
        {
            using (var client = new AmazonDynamoDBClient())
            {
                using (var context = new DynamoDBContext(client))
                {
                    var record = await context.LoadAsync<AdvertDbModel>(model.Id);
                    if (record == null)
                    {
                        throw new KeyNotFoundException($"A record with ID={model.Id} was not found.");
                    }
                    if (model.Status == AdvertStatus.Active)
                    {
                        record.Status = AdvertStatus.Active;
                        await context.SaveAsync(record);
                    }
                    else
                    {
                        await context.DeleteAsync(record);
                    }
                }
            }
        }

        public async Task<List<AdvertModel>> GetAll()
        {
            using (var client = new AmazonDynamoDBClient())
            {
                using (var context = new DynamoDBContext(client))
                {
                    var allItems = await context.ScanAsync<AdvertDbModel>(new List<ScanCondition>()).GetRemainingAsync();
                    return allItems.Select(item => _mapper.Map<AdvertModel>(item)).ToList();
                }
            }
        }

        public async Task<AdvertModel> GetById(string id)
        {
            using (var client = new AmazonDynamoDBClient())
            {
                using (var context = new DynamoDBContext(client))
                {
                    var record = await context.LoadAsync<AdvertDbModel>(id);
                    if (record == null)
                    {
                        throw new KeyNotFoundException($"A record with ID={id} was not found.");
                    }

                    AdvertModel model;

                    try
                    {
                        model = _mapper.Map<AdvertModel>(record);
                    }
                    catch (Exception ex)
                    {
                        throw;
                    }

                    return model;
                }
            }
        }
    }
}
